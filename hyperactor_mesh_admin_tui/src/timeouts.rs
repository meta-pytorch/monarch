/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Central timeout policy for the mesh admin TUI.
//!
//! # Timeout policy invariants (TP-*)
//!
//! - **TP-1:** Every networked TUI operation has a named policy
//!   entry.
//! - **TP-2:** Refresh cadence and request timeout are distinct
//!   concepts.
//! - **TP-4:** Diagnostics thresholds are policy-backed, not
//!   file-local.
//! - **TP-5:** Timeout policy is recorded at operation boundaries.
//! - **TP-6:** `TuiTimeoutPolicy::from_config` produces
//!   operation-specific request budgets.
//! - **TP-7:** No client-level timeout. The `reqwest::Client` is
//!   built without `.timeout()`. All timeout enforcement is
//!   per-operation via `tokio::time::timeout` at the call boundary.
//! - **TP-8:** Diagnostics uses only policy-provided thresholds and
//!   budgets.
//! - **TP-9:** Each request operation enforces its own budget via
//!   `tokio::time::timeout` at the operation boundary.
//! - **TP-10:** The effective refresh policy is derived from active
//!   job state. `RefreshPolicy` implements `JoinSemilattice` so
//!   multiple sources can be combined when added. Background refresh
//!   is suspended while a foreground operation is in flight.

use std::time::Duration;

use crate::TuiConfig;

/// Request-level operations. Each has a per-operation timeout
/// enforced via `tokio::time::timeout` at the call boundary (TP-9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestOp {
    /// Topology refresh, node detail, expand.
    InteractiveFetch,
    /// `GET /v1/config/{proc_reference}`.
    ConfigDump,
    /// `GET /v1/pyspy/{proc_reference}`.
    PySpyDump,
}

/// All [`RequestOp`] variants, for iteration in law-based tests.
#[cfg(test)]
pub(crate) const ALL_REQUEST_OPS: &[RequestOp] = &[
    RequestOp::InteractiveFetch,
    RequestOp::ConfigDump,
    RequestOp::PySpyDump,
];

/// Per-probe budgets, applied via `tokio::time::timeout` inside the
/// probe function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeOp {
    /// Individual diagnostics health check.
    DiagnosticsProbe,
}

/// Whole-job ceilings, applied via `tokio::time::timeout` around the
/// entire job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkflowOp {
    /// Full diagnostic suite ceiling.
    DiagnosticsRun,
}

/// Central timeout policy for the TUI. Every timing decision flows
/// through this struct.
///
/// `Copy + Clone` — pass by value at top-level, borrow internally. No
/// `Default` impl; callers must go through `from_config`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TuiTimeoutPolicy {
    /// Base refresh cadence from `--refresh-ms`.
    /// Separation law: not used in any timeout accessor.
    pub refresh_interval: Duration,
    /// Latency classification threshold for successful diagnostics
    /// probes: below this is `Pass`, at or above this is `Slow`.
    /// This is not a timeout budget and does not cancel the probe.
    pub diagnostics_probe_slow: Duration,
    // Private fields; accessed via typed accessors.
    interactive_fetch: Duration,
    config_dump: Duration,
    pyspy_dump: Duration,
    diagnostics_probe: Duration,
    diagnostics_run: Duration,
}

impl TuiTimeoutPolicy {
    /// Construct timeout policy from TUI config plus selected global
    /// mesh-admin config attrs.
    ///
    /// `refresh_interval` comes from `TuiConfig.refresh_ms`.
    ///
    /// Request budgets are operation-specific (TP-9). There is no
    /// shared client-level timeout; all enforcement is per-operation
    /// via `tokio::time::timeout` at the call boundary (TP-7).
    ///
    /// Diagnostics budgets preserve the existing effective values
    /// from the pre-policy implementation.
    pub fn from_config(config: &TuiConfig) -> Self {
        Self {
            refresh_interval: Duration::from_millis(config.refresh_ms),
            diagnostics_probe_slow: Duration::from_millis(500),
            interactive_fetch: Duration::from_secs(5),
            config_dump: Duration::from_secs(8),
            pyspy_dump: hyperactor_config::global::get(
                hyperactor_mesh::config::MESH_ADMIN_PYSPY_CLIENT_TIMEOUT,
            ),
            diagnostics_probe: Duration::from_secs(5),
            diagnostics_run: Duration::from_secs(120),
        }
    }

    /// Per-request timeout for an HTTP-backed operation.
    pub fn request_timeout(&self, op: RequestOp) -> Duration {
        match op {
            RequestOp::InteractiveFetch => self.interactive_fetch,
            RequestOp::ConfigDump => self.config_dump,
            RequestOp::PySpyDump => self.pyspy_dump,
        }
    }

    /// Per-probe timeout for a diagnostic probe.
    pub fn probe_timeout(&self, op: ProbeOp) -> Duration {
        match op {
            ProbeOp::DiagnosticsProbe => self.diagnostics_probe,
        }
    }

    /// Whole-job ceiling for a workflow operation.
    pub fn workflow_timeout(&self, op: WorkflowOp) -> Duration {
        match op {
            WorkflowOp::DiagnosticsRun => self.diagnostics_run,
        }
    }
}

/// Refresh interaction policy for background topology updates.
///
/// Phase 3a implements the minimal carrier:
/// `Baseline < Suspend`.
///
/// The policy is a join-semilattice so refresh constraints can be
/// combined compositionally rather than encoded as ad hoc event-loop
/// conditionals. In this first version, foreground jobs contribute
/// either `Baseline` or `Suspend`. Later phases may add additional
/// sources or richer policies such as `Degrade(...)` without changing
/// the combination model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RefreshPolicy {
    /// Run at the user-configured refresh cadence.
    Baseline,
    /// Do not schedule background refresh.
    Suspend,
}

impl algebra::JoinSemilattice for RefreshPolicy {
    /// Join chooses the more restrictive refresh policy.
    ///
    /// This is intentionally defined in algebraic form so that future
    /// extensions can combine multiple refresh-pressure sources by join
    /// rather than bespoke branching.
    fn join(&self, other: &Self) -> Self {
        std::cmp::max(*self, *other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> TuiConfig {
        TuiConfig {
            addr: "localhost:1729".to_string(),
            refresh_ms: 2000,
            theme: crate::ThemeName::Nord,
            lang: crate::theme::LangName::En,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
            diagnose: false,
        }
    }

    // TP-6/TP-9: per-operation request budgets.
    #[test]
    fn from_config_request_budget_interactive_fetch() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(
            policy.request_timeout(RequestOp::InteractiveFetch),
            Duration::from_secs(5),
        );
    }

    // TP-6/TP-9: config dump budget.
    #[test]
    fn from_config_request_budget_config_dump() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(
            policy.request_timeout(RequestOp::ConfigDump),
            Duration::from_secs(8),
        );
    }

    // TP-6/TP-9: py-spy budget preserves MESH_ADMIN_PYSPY_CLIENT_TIMEOUT.
    #[test]
    fn from_config_request_budget_pyspy_dump() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        let expected = hyperactor_config::global::get(
            hyperactor_mesh::config::MESH_ADMIN_PYSPY_CLIENT_TIMEOUT,
        );
        assert_eq!(policy.request_timeout(RequestOp::PySpyDump), expected);
    }

    // TP-6: probe budget.
    #[test]
    fn from_config_preserves_probe_timeout() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(
            policy.probe_timeout(ProbeOp::DiagnosticsProbe),
            Duration::from_secs(5),
        );
    }

    // TP-6: workflow ceiling.
    #[test]
    fn from_config_preserves_workflow_timeout() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(
            policy.workflow_timeout(WorkflowOp::DiagnosticsRun),
            Duration::from_secs(120),
        );
    }

    // TP-6: slow classification threshold.
    #[test]
    fn from_config_preserves_slow_threshold() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(policy.diagnostics_probe_slow, Duration::from_millis(500));
    }

    // TP-2: separation law — refresh_interval is independent of
    // request timeouts.
    #[test]
    fn refresh_interval_independent_of_timeouts() {
        let mut config = default_config();
        config.refresh_ms = 500;
        let policy = TuiTimeoutPolicy::from_config(&config);
        assert_eq!(policy.refresh_interval, Duration::from_millis(500));
        // All timeout accessors unchanged.
        let default_policy = TuiTimeoutPolicy::from_config(&default_config());
        for op in ALL_REQUEST_OPS {
            assert_eq!(
                policy.request_timeout(*op),
                default_policy.request_timeout(*op),
            );
        }
        assert_eq!(
            policy.probe_timeout(ProbeOp::DiagnosticsProbe),
            default_policy.probe_timeout(ProbeOp::DiagnosticsProbe),
        );
        assert_eq!(
            policy.workflow_timeout(WorkflowOp::DiagnosticsRun),
            default_policy.workflow_timeout(WorkflowOp::DiagnosticsRun),
        );
    }

    // TP-2: default cadence from --refresh-ms.
    #[test]
    fn refresh_interval_default_cadence() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(policy.refresh_interval, Duration::from_secs(2));
    }

    // TP-10: RefreshPolicy semilattice laws. Intentionally minimal
    // because the carrier is intentionally minimal in Phase 3a.
    // Extend when the carrier grows.

    use algebra::JoinSemilattice;

    #[test]
    fn refresh_policy_join_baseline_suspend() {
        assert_eq!(
            RefreshPolicy::Baseline.join(&RefreshPolicy::Suspend),
            RefreshPolicy::Suspend,
        );
    }

    #[test]
    fn refresh_policy_join_baseline_baseline() {
        assert_eq!(
            RefreshPolicy::Baseline.join(&RefreshPolicy::Baseline),
            RefreshPolicy::Baseline,
        );
    }

    #[test]
    fn refresh_policy_join_suspend_baseline() {
        assert_eq!(
            RefreshPolicy::Suspend.join(&RefreshPolicy::Baseline),
            RefreshPolicy::Suspend,
        );
    }

    #[test]
    fn refresh_policy_join_suspend_suspend() {
        assert_eq!(
            RefreshPolicy::Suspend.join(&RefreshPolicy::Suspend),
            RefreshPolicy::Suspend,
        );
    }
}
