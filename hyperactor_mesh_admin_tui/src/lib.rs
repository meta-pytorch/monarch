/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Interactive TUI client for the Monarch mesh admin HTTP API.
//!
//! Displays the mesh topology as a navigable tree by walking `GET
//! /v1/{reference}` endpoints. Selecting any node shows contextual
//! details on the right pane, including actor flight recorder events
//! when an actor is selected.
//!
//! Binaries construct a [`TuiConfig`] (after resolving any
//! `mast_conda:///` handles themselves) and call [`run`].
//!
//! # Design Pillars (Algebraic)
//!
//! This TUI is intentionally structured around three algebraic
//! invariants to make behavior correct by construction:
//!
//! 1. **TUI-1 (join-semilattice):** All fetch results merge via
//!    `FetchState::join`, guaranteeing commutativity, associativity,
//!    and idempotence under retries and reordering.
//! 2. **TUI-2 (cursor-bounds):** Selection is managed by `Cursor`, which
//!    enforces the invariant `pos < len` (or `pos == 0` when empty).
//! 3. **TUI-3 (tree-recursion):** The mesh topology is stored
//!    as an explicit tree (`TreeNode { children }`) and rendered via
//!    a pure projection (`flatten_tree`), avoiding ad-hoc list
//!    surgery.
//!
//! Additional invariants enforced throughout the code:
//!
//! - **TUI-4 (failed-always-visible):** Failed nodes are always
//!   visible regardless of the `show_stopped` toggle.
//! - **TUI-5 (single-fetch-path):** All cache writes go through
//!   `fetch_with_join` (no direct inserts).
//! - **TUI-6 (refresh-staleness):** `FetchState::Ready` with
//!   `generation < refresh_gen` is refetched; errors always retry.
//! - **TUI-7 (synthetic-root):** The root node is synthetic and
//!   always expanded; only its children are rendered at depth 0.
//! - **TUI-8 (cycle-safety):** Tree building rejects only true
//!   cycles (nodes that appear in their own ancestor path).
//! - **TUI-9 (depth-cap):** Recursion is bounded by
//!   `MAX_TREE_DEPTH` (Root→Host→Proc→Actor→ChildActor).
//! - **TUI-10 (fold-traversal):** Walks over `TreeNode` use fold
//!   abstractions, not bespoke recursion.
//! - **TUI-11 (selection-semantics):** Cursor restoration prefers
//!   `(reference, depth)` to disambiguate; falls back to
//!   reference-only if depth changes.
//! - **TUI-12 (serial-topology-fetches):** Topology cache fetches
//!   (via `fetch_with_join` / `build_tree_node`) are serial within
//!   a refresh cycle; join semantics handle retries and reordering.
//!   Overlay fetches (py-spy, config, diagnostics) are concurrent
//!   via `tokio::spawn` and do not participate in the topology cache.
//! - **TUI-13 (stopped-detection):** `is_stopped_node` matches
//!   `Actor` variants whose `actor_status` starts with `"stopped:"`
//!   or `"failed:"`. All other variants return false.
//! - **TUI-14 (stopped-dual-source):** `TreeNode.stopped` is true
//!   if the cached payload is stopped OR the child ref appears in
//!   the parent proc's `stopped_children` list.
//! - **TUI-15 (filter-order):** When both filters are off, system
//!   membership is checked first.
//! - **TUI-16 (placeholder-equivalence):** `placeholder_stopped` is
//!   identical to `placeholder` except `stopped: true`.
//! - **TUI-17 (failure-propagation):** A node's `failed` flag is
//!   `is_failed_node(payload) || children.any(failed)`. Host/root
//!   nodes are failed only when a descendant is.
//! - **TUI-18 (collapsed-failure-carry):** When collapsed, prior
//!   `failed` state is carried forward via `failed_keys`. Expanded
//!   nodes always recompute from live children.
//! - **TUI-19 (system-dual-source):** `TreeNode.is_system` is true
//!   if `is_system: true` in payload OR child ref in parent's
//!   `system_children`. Style precedence: selected > stopped >
//!   system > node-type.
//! - **TUI-20 (proc-accounting):** Displayed total is `num_actors +
//!   stopped_children.len()`. `"(max retained)"` appears iff cap
//!   reached.
//!
//! TLS and transport invariants:
//!
//! - **TUI-T1 (tls-auto-detect):** `client::build_client` probes
//!   for TLS material in priority order: explicit CLI paths →
//!   `try_tls_pem_bundle` → plain HTTP fallback.
//! - **TUI-T2 (prebuilt-client):** `App::new` receives a pre-built
//!   `reqwest::Client` and `base_url` (including scheme). TLS
//!   configuration is external to the app state.
//! - **TUI-T3 (scheme-inclusive-url):** `base_url` always starts
//!   with `http://` or `https://`; bare `host:port` is resolved to
//!   a scheme during client construction, never stored schemeless.
//! - **TUI-21 (job-overlay-coherence):** `active_job.is_some() ↔
//!   overlay.is_some()`. Enforced by `set_job`/`dismiss_job` —
//!   all creation goes through `set_job` (which calls
//!   `rebuild_overlay`) and all teardown through `dismiss_job`
//!   (which clears both fields). Makes it structurally impossible
//!   to have an orphaned overlay or a running job with no display
//!   surface.
//!
//! Py-spy overlay invariants:
//!
//! - **PY-1 (fresh-trace):** Every `p` press issues a new HTTP
//!   fetch; no cached py-spy result is ever reused.
//! - **PY-2 (overlay-ownership):** A py-spy result may only populate
//!   a still-valid active py-spy overlay. Stale results are
//!   invalidated structurally: `active_job` carries the receiver
//!   inside the `PySpy` variant, so replacing or clearing `active_job`
//!   drops the receiver and cancels any in-flight fetch. TUI-21
//!   guarantees `active_job` is always `None` when `overlay` is `None`.
//! - **PY-3 (replacement):** Opening or refreshing py-spy replaces
//!   prior overlay content and resets scroll to zero on result
//!   arrival.
//! - **PY-4 (selection-totality):** `p` is a no-op on Root/Host;
//!   targets the proc ref directly on Proc; targets the owning proc
//!   via `detail.parent` on Actor.
//! - **PY-5 (overlay-isolation):** Diagnostics and py-spy overlays
//!   must not write into each other's display surface. Enforced by
//!   `set_job`: `RunDiagnostics` calls `set_job` with the
//!   `Diagnostics` variant (dropping any live `PySpy` receiver);
//!   Esc calls `dismiss_job`; `recv_active_job` fires only for the
//!   variant currently stored.
//!
//! Config overlay invariants:
//!
//! - **CFG-1 (fresh-snapshot):** Every `C` press issues a new HTTP
//!   fetch; no cached config result is ever reused.
//! - **CFG-2 (overlay-ownership):** A config result may only populate
//!   a still-valid active config overlay. Stale results are
//!   invalidated structurally: `active_job` carries the receiver
//!   inside the `Config` variant, so replacing or clearing `active_job`
//!   drops the receiver and cancels any in-flight fetch. TUI-21
//!   guarantees `active_job` is always `None` when `overlay` is `None`.
//! - **CFG-3 (replacement):** Opening or refreshing config replaces
//!   prior overlay content and resets scroll to zero on result
//!   arrival.
//! - **CFG-4 (selection-totality):** `C` targets the proc ref directly
//!   on Proc (worker or service); targets the owning proc via
//!   `detail.parent` on Actor; no-op on Root/Host. Backend routes to
//!   ProcAgent or HostAgent per proc type (same as PY-4).
//! - **CFG-5 (overlay-isolation):** Config, Diagnostics, and PySpy
//!   overlays must not write into each other's display surface.
//!   Enforced by `set_job`: each variant drops any prior receiver;
//!   `recv_active_job` fires only for the variant currently stored.
//!
//! Laziness + recursion benefits:
//! - **Lazy expansion**: proc/actor children are placeholders until
//!   expanded, keeping refresh costs bounded and scaling work to what
//!   the user explores.
//! - **Structural recursion**: tree operations are defined by
//!   recursion/folds over the explicit tree, avoiding brittle
//!   index-based manipulation and making the view a pure projection.
//!
//! ```bash
//! # Terminal 1: Run dining philosophers (or any hyperactor application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_example_dining_philosophers
//!
//! # Terminal 2: Run this TUI (use the port printed by the application)
//! buck2 run fbcode//monarch/hyperactor_mesh_admin_tui:hyperactor_mesh_admin_tui -- --addr 127.0.0.1:XXXXX
//! ```

// tokio is used extensively (tokio::spawn, tokio::time, tokio::sync)
// but the unused-deps linter does not see through fbinit's runtime
// provider. This suppresses the false positive while keeping tokio
// in BUCK deps for autocargo.
use tokio as _;

mod actions;
mod app;
pub(crate) mod client;
mod diagnostics;
mod fetch;
mod filter;
mod format;
mod job;
mod model;
mod overlay;
mod render;
mod theme;
pub(crate) mod timeouts;
mod tree;

#[cfg(test)]
mod tests;

// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use std::collections::HashMap;
#[allow(unused_imports)]
pub(crate) use std::collections::HashSet;
use std::io;
use std::io::IsTerminal;
use std::time::Duration;

pub(crate) use actions::*;
pub(crate) use app::*;
use crossterm::ExecutableCommand;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use crossterm::terminal::disable_raw_mode;
use crossterm::terminal::enable_raw_mode;
pub(crate) use fetch::*;
pub(crate) use filter::*;
pub(crate) use format::*;
// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use hyperactor_mesh::introspect::NodePayload;
#[allow(unused_imports)]
pub(crate) use hyperactor_mesh::introspect::NodeProperties;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
pub(crate) use job::*;
pub(crate) use model::*;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use render::*;
// --- public API ---
pub use theme::LangName;
pub use theme::ThemeName;
pub(crate) use theme::*;
pub(crate) use tree::*;

/// Configuration for the mesh admin TUI.
///
/// Binaries translate their own CLI args into this struct.
/// The `addr` field must be a resolved address (`host:port` or
/// `https://host:port`), not a MAST handle. MAST resolution is
/// the binary's responsibility.
pub struct TuiConfig {
    pub addr: String,
    pub refresh_ms: u64,
    pub theme: ThemeName,
    pub lang: LangName,
    pub tls_ca: Option<String>,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub diagnose: bool,
}

// Terminal setup / teardown

/// Put the terminal into "TUI mode".
fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

/// Restore the terminal back to normal "shell mode".
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

async fn run_diagnose(
    client: reqwest::Client,
    base_url: String,
    policy: timeouts::TuiTimeoutPolicy,
) -> io::Result<()> {
    use crate::diagnostics::DiagSummary;
    use crate::diagnostics::run_diagnostics;

    let mut rx = run_diagnostics(client, base_url, &policy);
    let mut results = Vec::new();

    let timed_out = tokio::time::timeout(
        policy.workflow_timeout(timeouts::WorkflowOp::DiagnosticsRun),
        async {
            while let Some(r) = rx.recv().await {
                results.push(r);
            }
        },
    )
    .await
    .is_err();

    let s = DiagSummary::from_results(&results);
    let healthy = s.passed == s.total && !timed_out;

    let report = serde_json::json!({
        "checks": results,
        "timed_out": timed_out,
        "summary": {
            "total": s.total,
            "passed": s.passed,
            "failed": s.total - s.passed,
            "admin_infra_passed": s.admin_passed,
            "admin_infra_total": s.admin_total,
            "mesh_passed": s.mesh_passed,
            "mesh_total": s.mesh_total,
            "healthy": healthy,
        }
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"))
    );

    if !healthy {
        std::process::exit(1);
    }
    Ok(())
}

/// Run the mesh admin TUI. Does not return until the user exits
/// or diagnostics complete.
pub async fn run(config: TuiConfig) -> io::Result<()> {
    let policy = timeouts::TuiTimeoutPolicy::from_config(&config);
    let (base_url, client) = client::build_client(&config, &policy);

    if config.diagnose {
        return run_diagnose(client, base_url, policy).await;
    }

    if !io::stdout().is_terminal() {
        eprintln!("This TUI requires a real terminal.");
        return Ok(());
    }

    let mut app = App::new(base_url, client, config.theme, config.lang, policy);
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid template"),
    );
    spinner.set_message(format!("mesh-admin — Connecting to {} ...", app.base_url));
    spinner.enable_steady_tick(Duration::from_millis(80));

    let splash_start = tokio::time::Instant::now();
    app.refresh().await;
    let elapsed = splash_start.elapsed();
    let min_splash = Duration::from_secs(2);
    if elapsed < min_splash {
        tokio::time::sleep(min_splash - elapsed).await;
    }

    spinner.finish_and_clear();

    let mut terminal = setup_terminal()?;
    let result = run_app(&mut terminal, app).await;
    restore_terminal(&mut terminal)?;
    result
}
