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
//! # Design Pillars (Algebraic)
//!
//! This TUI is intentionally structured around three algebraic
//! invariants to make behavior correct by construction:
//!
//! 1. **Join-semilattice cache**: all fetch results merge via
//!    `FetchState::join`, guaranteeing commutativity, associativity,
//!    and idempotence under retries and reordering.
//! 2. **Cursor laws**: selection is managed by `Cursor`, which
//!    enforces the invariant `pos < len` (or `pos == 0` when empty).
//! 3. **Tree as structural recursion**: the mesh topology is stored
//!    as an explicit tree (`TreeNode { children }`) and rendered via
//!    a pure projection (`flatten_tree`), avoiding ad-hoc list
//!    surgery.
//!
//! Additional invariants enforced throughout the code:
//! - **Single fetch+join path**: all cache writes go through
//!   `fetch_with_join` (no direct inserts).
//! - **Refresh staleness**: `FetchState::Ready` with `generation <
//!   refresh_gen` is refetched; errors always retry.
//! - **Synthetic root**: the root node is synthetic and always
//!   expanded; only its children are rendered at depth 0.
//! - **Cycle safety**: tree building rejects only true cycles (nodes
//!   that appear in their own ancestor path).
//! - **Depth cap**: recursion is bounded by `MAX_TREE_DEPTH`.
//!   This limits traversal to Root→Host→Proc→Actor→ChildActor, keeps
//!   stack depth small, and avoids runaway fetches on deep graphs.
//! - **Tree-structure traversal via folds**: walks over the `TreeNode`
//!   structure use fold abstractions (`fold_tree`, `fold_tree_mut`, or
//!   `fold_tree_mut_with_depth`), not bespoke recursion. The flattened
//!   row list (`VisibleRows`) is iterated directly for rendering and
//!   event handling.
//! - **Selection semantics**: cursor restoration prefers
//!   `(reference, depth)` to disambiguate duplicate references, and
//!   falls back to reference-only matching if depth changes (e.g.,
//!   parent expanded/collapsed between refreshes).
//! - **Concurrency model**: HTTP fetches are scheduled serially
//!   through the event loop; in-flight requests are not explicitly
//!   cancelled or serialized, so slow responses may overlap. Join
//!   semantics handle retries and reordering.
//! - **Stopped detection is actor-only and prefix-based**:
//!   `is_stopped_node` matches `Actor` variants whose `actor_status`
//!   starts with `"stopped:"` or `"failed:"`. All other variants
//!   return false.
//! - **Stopped filtering is dual-source (OR)**: `TreeNode.stopped`
//!   is true if the cached payload is stopped OR the child ref
//!   appears in the parent proc's `stopped_children` list. Either
//!   source alone is sufficient.
//! - **Filter order: system before stopped**: when both filters are
//!   off, system membership is checked first. A node that is both
//!   system and stopped is eliminated by the system check.
//! - **Placeholder structural equivalence**: `placeholder_stopped`
//!   is identical to `placeholder` except `stopped: true`. Stopped
//!   is a rendering hint, not an expansion barrier.
//! - **System actor styling is dual-source (OR)**: `TreeNode.is_system`
//!   is true if the cached payload reports `is_system: true` OR the
//!   child ref appears in the parent's `system_children` list.
//!   System actors render Blue; style precedence is
//!   selected > stopped > system > node-type.
//! - **Proc actor accounting**: displayed total is `num_actors +
//!   stopped_children.len()`. `num_actors` counts only live actors.
//!   `"(max retained)"` appears iff `stopped_retention_cap > 0` and
//!   `stopped_children.len() >= stopped_retention_cap`.
//!
//! TLS and transport invariants:
//! - **TLS auto-detection (client)**: `client::build_client` probes
//!   for TLS material in priority order: explicit CLI paths (`--tls-ca`,
//!   `--tls-cert`, `--tls-key`) → `hyperactor::channel::try_tls_pem_bundle`
//!   (OSS config attrs, then Meta well-known paths) → plain HTTP fallback.
//! - **Pre-built client injection**: `App::new` receives a pre-built
//!   `reqwest::Client` and `base_url` (including scheme). TLS
//!   configuration is external to the app state.
//! - **Scheme-inclusive base URL**: `base_url` always starts with
//!   `http://` or `https://`; bare `host:port` addresses are resolved
//!   to a scheme during client construction, never stored schemeless.
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
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr 127.0.0.1:XXXXX
//! ```

mod actions;
mod app;
mod client;
mod fetch;
mod filter;
mod format;
mod model;
mod render;
mod theme;
mod tree;

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
use clap::Parser;
use crossterm::ExecutableCommand;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use crossterm::terminal::disable_raw_mode;
use crossterm::terminal::enable_raw_mode;
pub(crate) use fetch::*;
pub(crate) use filter::*;
pub(crate) use format::*;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use hyperactor::introspect::NodePayload;
#[allow(unused_imports)]
pub(crate) use hyperactor::introspect::NodeProperties;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
pub(crate) use model::*;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
// Re-exports so #[cfg(test)] mod tests can use `use super::*`.
#[allow(unused_imports)]
pub(crate) use render::*;
pub(crate) use theme::*;
pub(crate) use tree::*;

// Terminal setup / teardown

/// Put the terminal into "TUI mode".
///
/// Enables raw mode, switches to the alternate screen, and clears it,
/// returning a `ratatui::Terminal` backed by crossterm.
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
///
/// Disables raw mode, leaves the alternate screen, and re-enables the
/// cursor.
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

// Main loop

#[cfg(fbcode_build)]
#[fbinit::main]
async fn main(_fb: fbinit::FacebookInit) -> io::Result<()> {
    run().await
}

#[cfg(not(fbcode_build))]
#[tokio::main]
async fn main() -> io::Result<()> {
    run().await
}

async fn run() -> io::Result<()> {
    #[allow(unused_mut)] // mut needed only in fbcode_build for mast:// resolution
    let mut args = Args::parse();

    if !io::stdout().is_terminal() {
        eprintln!("This TUI requires a real terminal.");
        return Ok(());
    }

    // Resolve mast_conda:/// handles to https://fqdn:port before
    // building the HTTP client. This is Meta-internal only; the OSS
    // build rejects it with an error message.
    if args.addr.starts_with("mast_conda:///") {
        #[cfg(fbcode_build)]
        {
            args.addr = client::resolve_mast_addr(&args.addr, args.admin_port).await;
        }
        #[cfg(not(fbcode_build))]
        {
            eprintln!("mast_conda:/// resolution requires the Meta-internal build");
            std::process::exit(1);
        }
    }

    // Build the HTTP client and base URL, configuring TLS when
    // certificates are available.
    let (base_url, client) = client::build_client(&args);

    // Show an indicatif spinner on stderr while fetching initial data.
    // This runs before the alternate screen so it's visible as a normal
    // Terminal line.
    let mut app = App::new(base_url, client, args.theme, args.lang);
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid template"),
    );
    spinner.set_message(format!("mesh-admin — Connecting to {} ...", app.base_url));
    spinner.enable_steady_tick(Duration::from_millis(80));

    let splash_start = RealClock.now();
    app.refresh().await;
    let elapsed = splash_start.elapsed();
    let min_splash = Duration::from_secs(2);
    if elapsed < min_splash {
        RealClock.sleep(min_splash - elapsed).await;
    }

    spinner.finish_and_clear();

    let mut terminal = setup_terminal()?;
    let result = run_app(&mut terminal, &args, app).await;
    restore_terminal(&mut terminal)?;
    result
}

#[cfg(test)]
mod tests;
