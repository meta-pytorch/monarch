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
//! - **TP-3:** Phase 1 preserves all effective timeout values
//!   exactly.
//! - **TP-4:** Diagnostics thresholds are policy-backed, not
//!   file-local.
//! - **TP-5:** Phase 1 records timeout policy at operation
//!   boundaries, even where enforcement still flows through the
//!   shared client timeout.
//! - **TP-6:** `TuiTimeoutPolicy::from_config` preserves current
//!   effective values in Phase 1.
//! - **TP-7:** `shared_client_timeout()` is derived only from
//!   `RequestOp`. `ProbeOp` and `WorkflowOp` never contaminate the
//!   client timeout. `build_client()` sources its timeout exclusively
//!   from `TuiTimeoutPolicy`, not directly from mesh-admin config
//!   attrs.
//! - **TP-8:** Diagnostics uses only policy-provided thresholds and
//!   budgets.

use std::time::Duration;

use crate::TuiConfig;

/// Request-level operations. Only these participate in
/// [`TuiTimeoutPolicy::shared_client_timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestOp {
    /// Topology refresh, node detail, expand.
    InteractiveFetch,
    /// `GET /v1/config/{proc_reference}`.
    ConfigDump,
    /// `GET /v1/pyspy/{proc_reference}`.
    PySpyDump,
}

/// All [`RequestOp`] variants, for iteration in laws and
/// `shared_client_timeout`.
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
    /// Phase 1 intentionally preserves the current request-timeout
    /// bug: interactive fetches, config dumps, and py-spy requests
    /// all inherit the shared `MESH_ADMIN_PYSPY_CLIENT_TIMEOUT`
    /// budget. Phase 2 splits these into operation-specific request
    /// budgets.
    ///
    /// Diagnostics budgets preserve the existing effective values
    /// from the pre-policy implementation.
    pub fn from_config(config: &TuiConfig) -> Self {
        let request_budget = hyperactor_config::global::get(
            hyperactor_mesh::config::MESH_ADMIN_PYSPY_CLIENT_TIMEOUT,
        );
        Self {
            refresh_interval: Duration::from_millis(config.refresh_ms),
            diagnostics_probe_slow: Duration::from_millis(500),
            interactive_fetch: request_budget,
            config_dump: request_budget,
            pyspy_dump: request_budget,
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

    /// Phase 1 compatibility shim: the shared `reqwest::Client`
    /// timeout, derived from request ops only so probe/workflow
    /// ceilings do not contaminate it (TP-7).
    ///
    /// Phase 2 is expected to eliminate or demote this by enforcing
    /// operation-specific request budgets at the call boundary
    /// instead of relying on one shared client timeout.
    pub fn shared_client_timeout(&self) -> Duration {
        ALL_REQUEST_OPS
            .iter()
            .map(|op| self.request_timeout(*op))
            .max()
            .unwrap()
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

    // TP-3/TP-6: extensional preservation — each accessor returns
    // today's effective timeout.
    #[test]
    fn from_config_preserves_request_timeouts() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        let expected = hyperactor_config::global::get(
            hyperactor_mesh::config::MESH_ADMIN_PYSPY_CLIENT_TIMEOUT,
        );
        assert_eq!(
            policy.request_timeout(RequestOp::InteractiveFetch),
            expected
        );
        assert_eq!(policy.request_timeout(RequestOp::ConfigDump), expected);
        assert_eq!(policy.request_timeout(RequestOp::PySpyDump), expected);
    }

    // TP-3/TP-6: extensional preservation — probe budget.
    #[test]
    fn from_config_preserves_probe_timeout() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(
            policy.probe_timeout(ProbeOp::DiagnosticsProbe),
            Duration::from_secs(5),
        );
    }

    // TP-3/TP-6: extensional preservation — workflow ceiling.
    #[test]
    fn from_config_preserves_workflow_timeout() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(
            policy.workflow_timeout(WorkflowOp::DiagnosticsRun),
            Duration::from_secs(120),
        );
    }

    // TP-3/TP-6: extensional preservation — slow classification threshold.
    #[test]
    fn from_config_preserves_slow_threshold() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        assert_eq!(policy.diagnostics_probe_slow, Duration::from_millis(500));
    }

    // TP-7: client law — shared_client_timeout derived only from
    // RequestOp, never from ProbeOp or WorkflowOp.
    #[test]
    fn shared_client_timeout_ge_all_request_ops() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        let client_t = policy.shared_client_timeout();
        for op in ALL_REQUEST_OPS {
            assert!(client_t >= policy.request_timeout(*op));
        }
    }

    // TP-7: workflow ceilings do not contaminate client timeout.
    #[test]
    fn shared_client_timeout_does_not_include_workflow() {
        let policy = TuiTimeoutPolicy::from_config(&default_config());
        let expected_request_budget = hyperactor_config::global::get(
            hyperactor_mesh::config::MESH_ADMIN_PYSPY_CLIENT_TIMEOUT,
        );
        assert_eq!(policy.shared_client_timeout(), expected_request_budget);
        assert_eq!(
            policy.workflow_timeout(WorkflowOp::DiagnosticsRun),
            Duration::from_secs(120),
        );
        assert_ne!(
            policy.shared_client_timeout(),
            policy.workflow_timeout(WorkflowOp::DiagnosticsRun),
        );
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
}
