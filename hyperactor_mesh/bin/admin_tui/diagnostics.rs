/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Self-diagnostic for the mesh admin TUI.
//!
//! Walks the full resolution graph (root → hosts → service proc
//! actors → first user proc → first user actor) and probes each node
//! via `GET /v1/{reference}`. Results stream through an
//! `mpsc::Receiver` so the TUI can render them live as each check
//! completes.
//!
//! # Failure domains
//!
//! Checks are tagged with a [`DiagPhase`]:
//!
//! - **[`DiagPhase::AdminInfra`]**: root, host agents, service proc,
//!   `agent[0]`, `mesh_admin[0]`, `mesh_admin_bridge[0]`. These test
//!   the admin layer itself independent of user workloads.
//! - **[`DiagPhase::Mesh`]**: first user proc and actor. These test
//!   whether the user's mesh is healthy.
//!
//! If Phase 1 passes but Phase 2 fails → mesh problem.
//! If Phase 1 fails → admin infra bug.

use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use hyperactor::clock::Clock;
use hyperactor::host::LOCAL_PROC_NAME;
use hyperactor::introspect::NodeProperties;
use hyperactor_mesh::host_mesh::host_agent::HOST_MESH_AGENT_ACTOR_NAME;
use hyperactor_mesh::mesh_admin::MESH_ADMIN_ACTOR_NAME;
use hyperactor_mesh::mesh_admin::MESH_ADMIN_BRIDGE_NAME;
use hyperactor_mesh::proc_agent::PROC_AGENT_ACTOR_NAME;
use hyperactor_mesh::proc_mesh::COMM_ACTOR_NAME;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::fetch::fetch_node_raw;

/// Which layer of the stack a diagnostic check exercises.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) enum DiagPhase {
    /// Root, host agents, service proc, and its actors — tests admin
    /// infra independent of user workloads.
    AdminInfra,
    /// First user proc and actor — tests whether the mesh itself is
    /// healthy.
    Mesh,
}

/// Outcome of a single diagnostic probe.
#[derive(Debug, Clone, Serialize)]
pub(crate) enum DiagOutcome {
    /// HTTP 200 received within [`SLOW_MS`] milliseconds.
    Pass { elapsed_ms: u64 },
    /// HTTP 200 received, but slower than [`SLOW_MS`].
    Slow { elapsed_ms: u64 },
    /// HTTP error, non-200 status, or timeout.
    Fail { elapsed_ms: u64, error: String },
}

/// The semantic role of a probed node, used to look up a localised
/// annotation in [`Labels`](crate::theme::Labels).
#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) enum DiagNodeRole {
    /// The admin HTTP server itself — the root of the resolution graph.
    AdminServer,
    /// A host agent process — one per machine, manages all procs.
    HostAgent,
    /// The system proc that houses the admin actor layer.
    AdminServiceProc,
    /// The `mesh_admin` actor that handles all `GET /v1/…` requests.
    IntrospectionHandler,
    /// The `agent` actor that manages actor spawn and lifecycle.
    ActorLifecycleManager,
    /// The `mesh_admin_bridge` instance that connects admin to the mesh.
    RootClientBridge,
    /// The local client proc — in-process, starts empty (LP-1). Gets a
    /// ProcAgent and root client actor only when activated via
    /// `this_proc()` (Python/Monarch).
    LocalClientProc,
    /// The `comm` actor on a user proc — enables proc-to-proc mesh messaging.
    CommActor,
    /// The `proc_agent` actor on a user proc — manages actor spawn and lifecycle.
    ProcAgent,
    /// The first non-system proc — confirms user workload is alive.
    UserProc,
    /// The first non-system actor in a user proc — reachable through full stack.
    UserActor,
}

/// Result of a single diagnostic probe.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct DiagResult {
    /// Human-readable check name shown in the TUI (e.g. `"agent[0]"`).
    pub(crate) label: String,
    /// Exact reference passed to `GET /v1/{reference}` — use this to
    /// reproduce the probe or identify the failing node directly.
    pub(crate) reference: String,
    /// Semantic role used to look up the localised annotation.
    pub(crate) note: Option<DiagNodeRole>,
    /// Which failure domain this check belongs to.
    pub(crate) phase: DiagPhase,
    /// Probe outcome.
    pub(crate) outcome: DiagOutcome,
}

// Response latency above which a pass is reported as slow.
const SLOW_MS: u64 = 500;

// Per-probe timeout. Set above the server's SINGLE_HOST_TIMEOUT (3 s)
// so server-side 504s are surfaced as Fail(error) rather than our own
// timeout.
const TIMEOUT_MS: u64 = 5000;

/// Aggregated pass/fail counts across both diagnostic phases.
/// Computed from a completed (or in-progress) result slice.
#[derive(Debug, Clone)]
pub(crate) struct DiagSummary {
    pub(crate) total: usize,
    /// Probes that returned Pass or Slow.
    pub(crate) passed: usize,
    pub(crate) admin_total: usize,
    pub(crate) admin_passed: usize,
    pub(crate) mesh_total: usize,
    pub(crate) mesh_passed: usize,
    /// True if any probe returned `DiagOutcome::Fail`.
    pub(crate) any_fail: bool,
}

impl DiagSummary {
    pub(crate) fn from_results(results: &[DiagResult]) -> Self {
        let is_pass =
            |o: &DiagOutcome| matches!(o, DiagOutcome::Pass { .. } | DiagOutcome::Slow { .. });
        Self {
            total: results.len(),
            passed: results.iter().filter(|r| is_pass(&r.outcome)).count(),
            admin_total: results
                .iter()
                .filter(|r| r.phase == DiagPhase::AdminInfra)
                .count(),
            admin_passed: results
                .iter()
                .filter(|r| r.phase == DiagPhase::AdminInfra && is_pass(&r.outcome))
                .count(),
            mesh_total: results
                .iter()
                .filter(|r| r.phase == DiagPhase::Mesh)
                .count(),
            mesh_passed: results
                .iter()
                .filter(|r| r.phase == DiagPhase::Mesh && is_pass(&r.outcome))
                .count(),
            any_fail: results
                .iter()
                .any(|r| matches!(r.outcome, DiagOutcome::Fail { .. })),
        }
    }
}

/// Run the full diagnostic suite against `base_url`.
///
/// Returns a channel receiver. The spawned task sends one
/// [`DiagResult`] as each probe completes (in walk order). The
/// channel closes when all probes finish, signalling completion.
pub(crate) fn run_diagnostics(
    client: reqwest::Client,
    base_url: String,
) -> mpsc::Receiver<DiagResult> {
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        walk(&client, &base_url, &tx).await;
    });
    rx
}

/// Send a single `DiagResult` and ignore send errors (TUI exited).
macro_rules! emit {
    ($tx:expr, $result:expr) => {
        let _ = $tx.send($result).await;
    };
}

/// Probe one reference and return both the `DiagResult` and (on
/// success) the fetched `NodePayload`.
async fn probe(
    client: &reqwest::Client,
    base_url: &str,
    label: impl Into<String>,
    reference: impl Into<String>,
    phase: DiagPhase,
) -> (DiagResult, Option<hyperactor::introspect::NodePayload>) {
    let label = label.into();
    let reference = reference.into();
    let t0 = Instant::now();

    let result = hyperactor::clock::RealClock
        .timeout(
            Duration::from_millis(TIMEOUT_MS),
            fetch_node_raw(client, base_url, &reference),
        )
        .await;

    let elapsed_ms = t0.elapsed().as_millis() as u64;

    let (outcome, payload) = match result {
        Ok(Ok(p)) if elapsed_ms >= SLOW_MS => (DiagOutcome::Slow { elapsed_ms }, Some(p)),
        Ok(Ok(p)) => (DiagOutcome::Pass { elapsed_ms }, Some(p)),
        Ok(Err(e)) => (
            DiagOutcome::Fail {
                elapsed_ms,
                error: e,
            },
            None,
        ),
        Err(_) => (
            DiagOutcome::Fail {
                elapsed_ms,
                error: format!("timed out after {}ms", TIMEOUT_MS),
            },
            None,
        ),
    };

    (
        DiagResult {
            label,
            reference,
            note: None,
            phase,
            outcome,
        },
        payload,
    )
}

/// Derive a short human-readable label from the resolved NodePayload.
fn label_from_payload(reference: &str, payload: &hyperactor::introspect::NodePayload) -> String {
    match &payload.properties {
        NodeProperties::Root { .. } => "root".to_string(),
        NodeProperties::Host { addr, .. } => addr.clone(),
        NodeProperties::Proc { proc_name, .. } => proc_name.clone(),
        NodeProperties::Actor { .. } => {
            // ActorId format: "proc_id,actor_name[rank]" — extract
            // the last comma-separated component.
            reference
                .rsplit(',')
                .next()
                .unwrap_or(reference)
                .to_string()
        }
        NodeProperties::Error { message, .. } => message.clone(),
    }
}

/// Classify the operational role of a system proc by name.
///
/// Uses naming convention as identity — consistent with
/// `hyperactor::host` construction. See LP-1.
fn proc_role(proc_name: &str) -> DiagNodeRole {
    if proc_name == LOCAL_PROC_NAME {
        DiagNodeRole::LocalClientProc
    } else {
        DiagNodeRole::AdminServiceProc
    }
}

/// Full diagnostic walk. Probes in order and emits results.
async fn walk(client: &reqwest::Client, base_url: &str, tx: &mpsc::Sender<DiagResult>) {
    // Phase 1 — Admin Infra

    // Root.
    let (mut result, root_payload) =
        probe(client, base_url, "root", "root", DiagPhase::AdminInfra).await;
    result.note = Some(DiagNodeRole::AdminServer);
    emit!(tx, result);
    let root_payload = match root_payload {
        Some(p) => p,
        None => return,
    };

    // Root-level system children are admin infrastructure managed by
    // the framework. Skip them here — they are not host agents and
    // their children are not user procs.
    let root_system_refs: HashSet<&str> = match &root_payload.properties {
        NodeProperties::Root {
            system_children, ..
        } => system_children.iter().map(|s| s.as_str()).collect(),
        _ => HashSet::new(),
    };

    for host_ref in root_payload
        .children
        .iter()
        .filter(|r| !root_system_refs.contains(r.as_str()))
    {
        // Host agent.
        let (mut r, host_payload) = probe(
            client,
            base_url,
            host_ref.as_str(),
            host_ref.as_str(),
            DiagPhase::AdminInfra,
        )
        .await;
        if let Some(p) = &host_payload {
            r.label = label_from_payload(host_ref, p);
        }
        r.note = Some(DiagNodeRole::HostAgent);
        emit!(tx, r);

        let host_payload = match host_payload {
            Some(p) => p,
            None => continue,
        };

        let system_refs: HashSet<&str> = match &host_payload.properties {
            NodeProperties::Host {
                system_children, ..
            } => system_children.iter().map(|s| s.as_str()).collect(),
            _ => HashSet::new(),
        };

        // Service proc(s) and their actors.
        for proc_ref in host_payload
            .children
            .iter()
            .filter(|r| system_refs.contains(r.as_str()))
        {
            let (mut r, proc_payload) = probe(
                client,
                base_url,
                proc_ref.as_str(),
                proc_ref.as_str(),
                DiagPhase::AdminInfra,
            )
            .await;
            if let Some(p) = &proc_payload {
                r.label = label_from_payload(proc_ref, p);
            }
            // Role is derived from proc_name via proc_role(). Only set
            // when the proc resolved successfully; leave None on fetch
            // failure so a bad probe isn't mislabelled.
            r.note = proc_payload.as_ref().and_then(|p| match &p.properties {
                NodeProperties::Proc { proc_name, .. } => Some(proc_role(proc_name)),
                _ => None,
            });
            emit!(tx, r);

            if let Some(proc_payload) = proc_payload {
                for actor_ref in &proc_payload.children {
                    let (mut r, payload) = probe(
                        client,
                        base_url,
                        actor_ref.as_str(),
                        actor_ref.as_str(),
                        DiagPhase::AdminInfra,
                    )
                    .await;
                    // Use fetched label if available; otherwise derive
                    // from ref string directly (same logic as
                    // label_from_payload for Actor).
                    if let Some(p) = &payload {
                        r.label = format!("  {}", label_from_payload(actor_ref, p));
                    } else {
                        let name = actor_ref.rsplit(',').next().unwrap_or(actor_ref.as_str());
                        r.label = format!("  {}", name);
                    }
                    let actor_name = actor_ref.rsplit(',').next().unwrap_or("");
                    r.note = if actor_name.starts_with(MESH_ADMIN_BRIDGE_NAME) {
                        Some(DiagNodeRole::RootClientBridge)
                    } else if actor_name.starts_with(MESH_ADMIN_ACTOR_NAME) {
                        Some(DiagNodeRole::IntrospectionHandler)
                    } else if actor_name.starts_with(HOST_MESH_AGENT_ACTOR_NAME) {
                        Some(DiagNodeRole::ActorLifecycleManager)
                    } else {
                        None
                    };
                    emit!(tx, r);
                }
            }
        }

        // Phase 2 — Mesh: every user proc, all its system actors, and
        // the first non-system actor as a representative user actor.
        for user_proc_ref in host_payload
            .children
            .iter()
            .filter(|r| !system_refs.contains(r.as_str()))
        {
            let (mut r, proc_payload) = probe(
                client,
                base_url,
                user_proc_ref.as_str(),
                user_proc_ref.as_str(),
                DiagPhase::Mesh,
            )
            .await;
            if let Some(p) = &proc_payload {
                r.label = label_from_payload(user_proc_ref, p);
            }
            r.note = Some(DiagNodeRole::UserProc);
            emit!(tx, r);

            if let Some(proc_payload) = proc_payload {
                let proc_system_refs: HashSet<&str> = match &proc_payload.properties {
                    NodeProperties::Proc {
                        system_children, ..
                    } => system_children.iter().map(|s| s.as_str()).collect(),
                    _ => HashSet::new(),
                };

                // Probe every system actor on this user proc.
                for actor_ref in proc_payload
                    .children
                    .iter()
                    .filter(|r| proc_system_refs.contains(r.as_str()))
                {
                    let (mut r, payload) = probe(
                        client,
                        base_url,
                        actor_ref.as_str(),
                        actor_ref.as_str(),
                        DiagPhase::Mesh,
                    )
                    .await;
                    if let Some(p) = &payload {
                        r.label = format!("  {}", label_from_payload(actor_ref, p));
                    } else {
                        let name = actor_ref.rsplit(',').next().unwrap_or(actor_ref.as_str());
                        r.label = format!("  {}", name);
                    }
                    let actor_name = actor_ref.rsplit(',').next().unwrap_or("");
                    r.note = if actor_name.starts_with(COMM_ACTOR_NAME) {
                        Some(DiagNodeRole::CommActor)
                    } else if actor_name.starts_with(PROC_AGENT_ACTOR_NAME) {
                        Some(DiagNodeRole::ProcAgent)
                    } else {
                        None
                    };
                    emit!(tx, r);
                }

                // Probe the first non-system actor as a representative.
                if let Some(actor_ref) = proc_payload
                    .children
                    .iter()
                    .find(|r| !proc_system_refs.contains(r.as_str()))
                {
                    let (mut r, payload) = probe(
                        client,
                        base_url,
                        actor_ref.as_str(),
                        actor_ref.as_str(),
                        DiagPhase::Mesh,
                    )
                    .await;
                    if let Some(p) = &payload {
                        r.label = format!("  {}", label_from_payload(actor_ref, p));
                    } else {
                        let name = actor_ref.rsplit(',').next().unwrap_or(actor_ref.as_str());
                        r.label = format!("  {}", name);
                    }
                    r.note = Some(DiagNodeRole::UserActor);
                    emit!(tx, r);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn phase_fail_count(results: &[DiagResult], phase: DiagPhase) -> usize {
        results
            .iter()
            .filter(|r| r.phase == phase && matches!(r.outcome, DiagOutcome::Fail { .. }))
            .count()
    }

    // Invariant LP-1: proc_role() maps proc names to roles by naming
    // convention. Pin this mapping so renames or new system procs are
    // caught at compile time.
    #[test]
    fn test_proc_role_classification() {
        assert!(matches!(
            proc_role(LOCAL_PROC_NAME),
            DiagNodeRole::LocalClientProc
        ));
        assert!(matches!(
            proc_role(hyperactor::host::SERVICE_PROC_NAME),
            DiagNodeRole::AdminServiceProc
        ));
        assert!(matches!(
            proc_role("anything_else"),
            DiagNodeRole::AdminServiceProc
        ));
    }

    // Invariant LP-1: the local proc starts empty in pure Rust. A Pass
    // result for LocalClientProc with no subsequent actor probes must
    // not contribute AdminInfra failures.
    #[test]
    fn test_empty_local_proc_does_not_degrade_admin_health() {
        let results = vec![DiagResult {
            label: "local".to_string(),
            reference: "some_ref".to_string(),
            note: Some(DiagNodeRole::LocalClientProc),
            phase: DiagPhase::AdminInfra,
            outcome: DiagOutcome::Pass { elapsed_ms: 1 },
        }];
        assert_eq!(phase_fail_count(&results, DiagPhase::AdminInfra), 0);
    }
}
