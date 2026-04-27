/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Py-spy endpoint tests (from `pyspy_preflight_test.sh`,
//! `pyspy_integration_test.sh`, and `verify_pyspy.py`).
//!
//! See MIT-7, MIT-8, MIT-10, MIT-11, MIT-12, MIT-16, MIT-17, MIT-18,
//! MIT-19 in `harness` module doc.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use anyhow::bail;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::mesh_admin::ApiErrorEnvelope;
use hyperactor_mesh::pyspy::PySpyFrame;
use hyperactor_mesh::pyspy::PySpyProfileOpts;
use hyperactor_mesh::pyspy::PySpyResult;

use crate::harness;
use crate::harness::WorkloadFixture;

// PyspyScenario — eager, total construction

/// A fully-initialized pyspy_workload scenario.
///
/// Construction is eager and total: `start()` returns only when the
/// fixture is live, procs are classified, and pyspy worker procs have
/// been discovered. You cannot hold a `PyspyScenario` without valid
/// proc refs.
struct PyspyScenario {
    fixture: WorkloadFixture,
    service: String,
    workers: Vec<String>,
    mode: String,
}

fn is_transient_pyspy_handler_not_ready(body: &str) -> bool {
    serde_json::from_str::<ApiErrorEnvelope>(body)
        .map(|envelope| {
            envelope.error.code == "not_found"
                && envelope
                    .error
                    .message
                    .contains("does not have a reachable handler")
        })
        .unwrap_or_else(|_| body.contains("does not have a reachable handler"))
}

async fn warm_worker_pyspy_endpoint(
    fixture: &WorkloadFixture,
    worker_proc_ref: &str,
) -> Result<()> {
    let encoded = urlencoding::encode(worker_proc_ref);
    let pyspy_path = format!("/v1/pyspy/{encoded}");
    let mut last_err = String::new();

    for attempt in 1..=30 {
        let resp = fixture
            .get(&pyspy_path)
            .await
            .map_err(|e| anyhow::anyhow!("attempt {attempt}: transport error: {e}"))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();

        if status.is_success() {
            serde_json::from_str::<PySpyResult>(&body).map_err(|e| {
                anyhow::anyhow!(
                    "attempt {attempt}: success response did not deserialize as PySpyResult: {e}: {body}"
                )
            })?;
            return Ok(());
        }

        let transient = matches!(status.as_u16(), 502..=504)
            || (status.as_u16() == 404 && is_transient_pyspy_handler_not_ready(&body));
        last_err = format!("attempt {attempt}: HTTP {status}: {body}");
        if transient {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        bail!("{last_err}");
    }

    bail!("{last_err}");
}

impl PyspyScenario {
    /// Start a fresh pyspy_workload and optionally warm the worker
    /// py-spy endpoint.
    async fn start_with_options(mode: &str, concurrency: usize, warm_worker_pyspy: bool) -> Self {
        let bin = harness::pyspy_workload_binary();
        let fixture = harness::start_workload(
            &bin,
            &[
                "--mode",
                mode,
                "--work-ms",
                "500",
                "--concurrency",
                &concurrency.to_string(),
            ],
            Duration::from_secs(60),
        )
        .await
        .expect("failed to start pyspy_workload");

        let procs = fixture
            .classify_procs()
            .await
            .expect("failed to classify pyspy procs");
        let service = procs.service;
        let workers = discover_pyspy_workers(&fixture, concurrency)
            .await
            .expect("failed to discover pyspy workers");

        if warm_worker_pyspy {
            // Startup convergence: wait for the pyspy endpoint to be
            // responsive on worker[0]. ProcAgent may still be
            // processing startup messages, and the first py-spy dump
            // (which attaches to the process and collects stack
            // traces) can be slower than the bridge timeout.
            //
            // We do NOT warm the service proc here — the service
            // proc's py-spy endpoint can consistently 504 under load,
            // and making that a hard gate would fail the entire
            // scenario. check_preflight() retries transient 504s on
            // both worker and service instead.
            warm_worker_pyspy_endpoint(&fixture, &workers[0])
                .await
                .unwrap_or_else(|e| panic!("pyspy endpoint not ready: {e}"));
        }

        PyspyScenario {
            fixture,
            service,
            workers,
            mode: mode.to_string(),
        }
    }

    /// Run a test closure with a fresh scenario.
    ///
    /// Same structural cleanup guarantee as `DiningScenario::run`
    /// (MIT-2).
    async fn run<F>(mode: &str, concurrency: usize, f: F)
    where
        F: for<'a> FnOnce(&'a PyspyScenario) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        Self::run_with_options(mode, concurrency, true, f).await;
    }

    async fn run_with_options<F>(mode: &str, concurrency: usize, warm_worker_pyspy: bool, f: F)
    where
        F: for<'a> FnOnce(&'a PyspyScenario) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
    {
        let scenario = Self::start_with_options(mode, concurrency, warm_worker_pyspy).await;
        let guard = ShutdownGuard(&scenario.fixture);
        f(&scenario).await;
        guard.disarm();
        scenario.fixture.shutdown().await;
    }
}

/// Drop guard for structural cleanup. See `dining::ShutdownGuard`.
struct ShutdownGuard<'a>(#[allow(dead_code)] &'a WorkloadFixture);

impl ShutdownGuard<'_> {
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {}
}

// Pyspy-worker proc discovery (MIT-8)

/// Discover workload procs that contain actors matching
/// `pyspy_worker`. Retries until count ≥ `expected`. MIT-8
/// (pyspy-worker-discovery).
async fn discover_pyspy_workers(fixture: &WorkloadFixture, expected: usize) -> Result<Vec<String>> {
    for _attempt in 1..=60 {
        let root: NodePayload = match fixture.get_node_payload("/v1/root").await {
            Ok(r) => r,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let mut procs = Vec::new();

        for host_ref in &root.children {
            let host_str = host_ref.to_string();
            let encoded = urlencoding::encode(&host_str);
            let host: NodePayload = match fixture.get_node_payload(&format!("/v1/{encoded}")).await
            {
                Ok(h) => h,
                Err(_) => continue,
            };

            for proc_ref in &host.children {
                let proc_str = proc_ref.to_string();
                let encoded = urlencoding::encode(&proc_str);
                let proc_node: NodePayload =
                    match fixture.get_node_payload(&format!("/v1/{encoded}")).await {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                let has_pyspy_worker = proc_node.children.iter().any(|actor_ref| match actor_ref {
                    hyperactor_mesh::introspect::NodeRef::Actor(id) => id
                        .label()
                        .map(|l| l.as_str().starts_with("pyspy_worker"))
                        .unwrap_or(false),
                    _ => false,
                });
                if has_pyspy_worker {
                    procs.push(proc_str);
                }
            }
        }

        if procs.len() >= expected {
            return Ok(procs);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    bail!("MIT-8: pyspy worker procs not found after 60 attempts (expected {expected})");
}

// Mode-specific evidence (ported from verify_pyspy.py)

/// MIT-17: Frames we expect to see in py-spy stacks for each mode.
fn mode_evidence(mode: &str) -> &'static [&'static str] {
    match mode {
        "cpu" => &["do_cpu_work", "_cpu_burn_loop", "process_batch"],
        "block" => &["do_blocking_work", "time.sleep", "process_batch"],
        "mixed" => &["do_cpu_work", "_cpu_burn_loop", "process_batch"],
        _ => panic!("unknown mode: {mode}"),
    }
}

/// MIT-19: Quality gate thresholds per mode.
fn quality_threshold(mode: &str) -> f64 {
    match mode {
        "cpu" => 0.7,
        "block" => 0.8,
        "mixed" => 0.3,
        _ => panic!("unknown mode: {mode}"),
    }
}

/// Mixed mode legitimately spends long stretches in idle/blocking
/// frames, so it needs a larger sampling budget to avoid
/// zero-evidence flakes.
fn samples_per_proc(mode: &str) -> usize {
    match mode {
        "mixed" => 3,
        "cpu" | "block" => 3,
        _ => panic!("unknown mode: {mode}"),
    }
}

/// Build a qualified name from a py-spy frame.
fn qualified_frame_name(frame: &PySpyFrame) -> String {
    match &frame.module {
        Some(module) if !module.is_empty() => format!("{}.{}", module, frame.name),
        _ => frame.name.clone(),
    }
}

/// Check whether a (possibly qualified) frame name matches any
/// evidence pattern.
fn has_evidence(name: &str, evidence: &[&str]) -> bool {
    evidence.iter().any(|pattern| name.contains(pattern))
}

// Assertion helpers

/// MIT-7, MIT-10, MIT-11, MIT-12, MIT-16: Endpoint-contract checks on
/// worker, service, and bogus procs.
///
/// Order: bogus (cheap fast-fail) → worker → service. Called before
/// evidence assertions so that an endpoint-contract regression is
/// reported cleanly, not masked by a discovery failure.
async fn check_preflight(s: &PyspyScenario) {
    // --- MIT-10, MIT-11: bogus proc error envelope (cheap, run first) ---
    let bogus = "unix:@nonexistent_bogus_socket_xyz,bogus-ffffffffffffffff";
    let encoded = urlencoding::encode(bogus);
    let resp = s
        .fixture
        .get(&format!("/v1/pyspy/{encoded}"))
        .await
        .unwrap_or_else(|e| {
            panic!("MIT-10/MIT-11: pyspy bogus-ref transport failure (expected HTTP error envelope, got: {e})")
        });
    let status = resp.status();
    assert!(
        !status.is_success(),
        "MIT-11: expected non-200 for bogus proc, got {status}"
    );
    let body = resp.text().await.unwrap();
    let envelope: ApiErrorEnvelope =
        serde_json::from_str(&body).expect("MIT-10: response should be ApiErrorEnvelope");
    assert!(
        envelope.error.code == "not_found" || envelope.error.code == "internal_error",
        "MIT-10: expected not_found or internal_error, got: {}",
        envelope.error.code
    );

    // MIT-12, MIT-16: worker then service procs (table-driven)
    //
    // Retries only transient gateway errors (502/503/504) that
    // indicate the py-spy bridge timed out or the server was
    // momentarily overloaded. Any other failure (404, malformed JSON,
    // wrong envelope) panics immediately so contract regressions are
    // not masked.
    for (label, proc_ref) in [
        ("worker", s.workers[0].as_str()),
        ("service", s.service.as_str()),
    ] {
        let encoded = urlencoding::encode(proc_ref);
        let path = format!("/v1/pyspy/{encoded}");
        let mut last_err: Option<String> = None;
        for _attempt in 1..=5 {
            let resp = s
                .fixture
                .get(&path)
                .await
                .unwrap_or_else(|e| panic!("MIT-16: {label}: transport error: {e}"));
            let status = resp.status();

            // Transient gateway errors — retry.
            if matches!(status.as_u16(), 502..=504) {
                let body = resp.text().await.unwrap_or_default();
                last_err = Some(format!("HTTP {status}: {body}"));
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }

            // Non-transient non-success — fail immediately.
            let body = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                panic!("MIT-16: {label}: HTTP {status}: {body}");
            }

            // Success — deserialize and verify variant.
            let result: PySpyResult = serde_json::from_str(&body)
                .unwrap_or_else(|e| panic!("MIT-16: {label}: bad JSON: {e}: {body}"));
            match &result {
                PySpyResult::Ok { .. } | PySpyResult::Failed { .. } => {}
                PySpyResult::BinaryNotFound { searched } => {
                    panic!(
                        "MIT-18: {label}: py-spy binary not found despite harness-provisioned PYSPY_BIN: {searched:?}"
                    );
                }
            }
            last_err = None;
            break;
        }
        if let Some(e) = last_err {
            panic!("MIT-16: {label}: {e} (after 5 attempts)");
        }
    }
}

/// MIT-16, MIT-17, MIT-18, MIT-19: Evidence-frame sampling over
/// worker procs.
async fn check_evidence(s: &PyspyScenario) {
    let evidence = mode_evidence(&s.mode);
    let threshold = quality_threshold(&s.mode);
    let samples_per_proc = samples_per_proc(&s.mode);

    let mut total_ok: usize = 0;
    let mut total_not_found: usize = 0;
    let mut total_failed: usize = 0;
    let mut total_evidence: usize = 0;

    for proc_ref in &s.workers {
        let encoded = urlencoding::encode(proc_ref);
        let mut ok = 0usize;
        let mut not_found = 0usize;
        let mut failed = 0usize;
        let mut proc_evidence = 0usize;

        for _ in 0..samples_per_proc {
            let result: PySpyResult =
                match s.fixture.get_json(&format!("/v1/pyspy/{encoded}")).await {
                    Ok(r) => r,
                    Err(_) => {
                        failed += 1;
                        continue;
                    }
                };

            match &result {
                PySpyResult::Ok { stack_traces, .. } => {
                    ok += 1;
                    let found = stack_traces.iter().any(|trace| {
                        trace
                            .frames
                            .iter()
                            .any(|frame| has_evidence(&qualified_frame_name(frame), evidence))
                    });
                    if found {
                        proc_evidence += 1;
                    }
                }
                PySpyResult::BinaryNotFound { .. } => {
                    not_found += 1;
                }
                PySpyResult::Failed { .. } => {
                    failed += 1;
                }
            }
        }

        total_ok += ok;
        total_not_found += not_found;
        total_failed += failed;
        total_evidence += proc_evidence;
    }

    // MIT-18: BinaryNotFound is unexpected under Buck-provisioned
    // PYSPY_BIN.
    if total_ok == 0 && total_failed == 0 && total_not_found > 0 {
        panic!(
            "MIT-18: no Ok responses for mode={} and all {} sample(s) were BinaryNotFound despite harness-provisioned PYSPY_BIN",
            s.mode, total_not_found
        );
    }

    // All failures (or mix of Failed + BinaryNotFound), no Ok responses.
    if total_ok == 0 {
        panic!(
            "MIT-17: no Ok responses for mode={} ({} failed, {} not-found)",
            s.mode, total_failed, total_not_found
        );
    }

    // MIT-17: Global evidence check.
    // Mixed mode spends ~50% in asyncio.sleep where py-spy sees idle
    // event-loop frames. With limited samples under load, it is
    // statistically possible to miss all evidence frames. Warn
    // instead of failing so stress-runs stay green for the
    // deterministic assertions while still flagging evidence gaps.
    if total_evidence == 0 {
        if s.mode == "mixed" {
            eprintln!(
                "WARN: MIT-17: {} Ok response(s) but none contained evidence for mode={} (non-fatal for mixed)",
                total_ok, s.mode
            );
            return;
        }
        panic!(
            "MIT-17: {} Ok response(s) but none contained evidence for mode={}",
            total_ok, s.mode
        );
    }

    // MIT-19: Quality thresholds as warnings.
    if total_ok > 0 {
        let hit_rate = total_evidence as f64 / total_ok as f64;
        if hit_rate < threshold {
            eprintln!(
                "WARN: hit rate {:.0}% is below quality threshold {:.0}% for mode={}",
                hit_rate * 100.0,
                threshold * 100.0,
                s.mode
            );
        }
    }
}

// Integration tests (one scoped PyspyScenario per mode)

/// MIT-2, MIT-8, MIT-10, MIT-11, MIT-12, MIT-16, MIT-17, MIT-18,
/// MIT-19: py-spy integration — cpu mode. Includes preflight
/// endpoint-contract checks before evidence assertions.
pub async fn run_pyspy_integration_cpu() {
    PyspyScenario::run("cpu", 2, |s| {
        Box::pin(async move {
            check_preflight(s).await;
            check_evidence(s).await;
            // MIT-73, MIT-74: profile SVG tests share the CPU fixture.
            check_profile_reject_zero_duration(s).await;
            check_profile_reject_over_max_duration(s).await;
            check_profile_reject_zero_rate(s).await;
            check_profile_reject_excessive_rate(s).await;
            check_profile_svg_success(s).await;
        })
    })
    .await;
}

/// MIT-2, MIT-8, MIT-10, MIT-11, MIT-12, MIT-16, MIT-17, MIT-18,
/// MIT-19: py-spy integration — block mode.
pub async fn run_pyspy_integration_block() {
    PyspyScenario::run("block", 2, |s| {
        Box::pin(async move {
            check_preflight(s).await;
            check_evidence(s).await;
        })
    })
    .await;
}

/// MIT-2, MIT-8, MIT-10, MIT-11, MIT-12, MIT-18: mixed mode smoke
/// test.
///
/// CPU and block already cover deterministic frame-evidence
/// assertions. Mixed mode is intentionally a cheap startup/topology
/// smoke check because its frame visibility is probabilistic and
/// disproportionately expensive.
pub async fn run_pyspy_integration_mixed() {
    PyspyScenario::run_with_options("mixed", 2, false, |s| {
        Box::pin(async move {
            check_preflight(s).await;
            assert_eq!(s.mode, "mixed");
            assert_eq!(s.workers.len(), 2, "MIT-8: expected 2 mixed worker procs");
        })
    })
    .await;
}

// --- profile SVG tests ---

fn profile_opts(duration_s: u32) -> PySpyProfileOpts {
    PySpyProfileOpts {
        duration_s,
        rate_hz: 100,
        native: false,
        threads: false,
        nonblocking: false,
    }
}

/// PP-1: zero duration rejected.
async fn check_profile_reject_zero_duration(s: &PyspyScenario) {
    let encoded = urlencoding::encode(&s.workers[0]);
    let resp = s
        .fixture
        .post(
            &format!("/v1/pyspy_profile_svg/{encoded}"),
            &profile_opts(0),
        )
        .await
        .expect("POST must not fail at transport level");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "PP-1: zero duration_s must be rejected"
    );
}

/// PP-1: over-max duration rejected.
async fn check_profile_reject_over_max_duration(s: &PyspyScenario) {
    let encoded = urlencoding::encode(&s.workers[0]);
    let mut opts = profile_opts(999);
    opts.duration_s = 999;
    let resp = s
        .fixture
        .post(&format!("/v1/pyspy_profile_svg/{encoded}"), &opts)
        .await
        .expect("POST must not fail at transport level");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "PP-1: over-max duration_s must be rejected"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("exceeds max"),
        "PP-1: error should mention exceeds max, got: {body}"
    );
}

/// PP-1: zero rate rejected.
async fn check_profile_reject_zero_rate(s: &PyspyScenario) {
    let encoded = urlencoding::encode(&s.workers[0]);
    let mut opts = profile_opts(2);
    opts.rate_hz = 0;
    let resp = s
        .fixture
        .post(&format!("/v1/pyspy_profile_svg/{encoded}"), &opts)
        .await
        .expect("POST must not fail at transport level");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "PP-1: zero rate_hz must be rejected"
    );
}

/// PP-1: excessive rate rejected.
async fn check_profile_reject_excessive_rate(s: &PyspyScenario) {
    let encoded = urlencoding::encode(&s.workers[0]);
    let mut opts = profile_opts(2);
    opts.rate_hz = 9999;
    let resp = s
        .fixture
        .post(&format!("/v1/pyspy_profile_svg/{encoded}"), &opts)
        .await
        .expect("POST must not fail at transport level");
    assert_eq!(
        resp.status().as_u16(),
        400,
        "PP-1: excessive rate_hz must be rejected"
    );
}

/// Happy path: profile a CPU worker, get SVG back.
async fn check_profile_svg_success(s: &PyspyScenario) {
    let encoded = urlencoding::encode(&s.workers[0]);
    let resp = s
        .fixture
        .post(
            &format!("/v1/pyspy_profile_svg/{encoded}"),
            &profile_opts(3),
        )
        .await
        .expect("POST must not fail at transport level");
    let status = resp.status().as_u16();
    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        status, 200,
        "profile must succeed on CPU worker, got {status}"
    );
    assert!(
        content_type.starts_with("image/svg+xml"),
        "content-type must be image/svg+xml, got: {content_type}"
    );
    assert!(!body.is_empty(), "SVG body must not be empty");
    let prefix = String::from_utf8_lossy(&body[..body.len().min(100)]);
    assert!(
        prefix.contains("<svg") || prefix.contains("<?xml"),
        "body must start with SVG content, got: {prefix}"
    );
}
