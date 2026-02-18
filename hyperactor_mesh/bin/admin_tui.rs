/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Interactive TUI client for the Monarch mesh admin HTTP API.
//!
//! Displays the mesh topology as a navigable tree by walking
//! `GET /v1/{reference}` endpoints. Selecting any node shows
//! contextual details on the right pane, including actor flight
//! recorder events when an actor is selected.
//!
//! ```bash
//! # Terminal 1: Run dining philosophers (or any hyperactor application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_example_dining_philosophers
//!
//! # Terminal 2: Run this TUI (use the port printed by the application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr 127.0.0.1:XXXXX
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::io::IsTerminal;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use clap::Parser;
use crossterm::ExecutableCommand;
use crossterm::event::Event;
use crossterm::event::EventStream;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyModifiers;
use crossterm::terminal::EnterAlternateScreen;
use crossterm::terminal::LeaveAlternateScreen;
use crossterm::terminal::disable_raw_mode;
use crossterm::terminal::enable_raw_mode;
use futures::StreamExt;
use hyperactor::ActorId;
use hyperactor::ProcId;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;
use hyperactor::introspect::RecordedEvent;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::List;
use ratatui::widgets::ListItem;
use ratatui::widgets::ListState;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Wrap;
use serde_json::Value;

/// Command-line arguments for the admin TUI.
#[derive(Debug, Parser)]
#[command(name = "admin-tui", about = "TUI client for hyperactor admin API")]
struct Args {
    /// Admin server address (e.g., 127.0.0.1:8080)
    #[arg(long, short)]
    addr: String,

    /// Refresh interval in milliseconds
    #[arg(long, default_value_t = 5000)]
    refresh_ms: u64,
}

// Topology tree model

/// Maximum recursion depth when walking references.
/// Root(skipped) → Host(0) → Proc(1) → Actor(2) → ChildActor(3).
const MAX_TREE_DEPTH: usize = 4;

/// Lightweight classification for a topology node, used for UI
/// concerns (primarily color-coding and a few display heuristics).
///
/// This is derived from the node's [`NodeProperties`] variant rather
/// than being persisted in the payload.
#[derive(Debug, Clone, Copy)]
enum NodeType {
    /// Synthetic root of the admin tree (not rendered as a row; hosts
    /// appear at depth 0).
    Root,
    /// A host in the mesh, identified by its admin-reported address.
    Host,
    /// A proc running on a host (system or user).
    Proc,
    /// An actor instance within a proc.
    Actor,
}

impl NodeType {
    /// Classify a node for UI purposes by mapping from its
    /// [`NodeProperties`] variant.
    ///
    /// This is a lossy projection: it preserves only the high-level
    /// kind (root/host/proc/actor), not the detailed fields (e.g.,
    /// `is_system`, counts, status).
    fn from_properties(props: &NodeProperties) -> Self {
        match props {
            NodeProperties::Root { .. } => NodeType::Root,
            NodeProperties::Host { .. } => NodeType::Host,
            NodeProperties::Proc { .. } => NodeType::Proc,
            NodeProperties::Actor { .. } => NodeType::Actor,
            NodeProperties::Error { .. } => NodeType::Actor,
        }
    }
}

/// A single row in the flattened topology view.
///
/// `TreeNode` is the UI model (not the authoritative topology): it
/// stores just enough information to render indentation,
/// expand/collapse state, and a stable `reference` key for fetching
/// the full [`NodePayload`] from the admin API.
#[derive(Debug, Clone)]
struct TreeNode {
    /// Human-friendly label shown in the tree (derived from
    /// [`NodePayload`]).
    label: String,
    /// Visual indentation level in the tree (0 = host under root).
    depth: usize,
    /// The reference string for this node (opaque identity).
    reference: String,
    /// Node type for color coding.
    node_type: NodeType,
    /// Whether this node is currently expanded in the UI.
    expanded: bool,
    /// Whether the backing payload reports any children (controls
    /// fold arrow rendering).
    has_children: bool,
}

// Application state

/// Runtime state for the admin TUI.
///
/// `App` owns the HTTP client, the currently materialized topology
/// tree, selection state, and a small cache of fetched
/// [`NodePayload`]s so navigation/detail rendering can be responsive
/// without re-fetching every node on every keypress.
struct App {
    /// Base URL for the admin server (e.g. `http://127.0.0.1:8080`).
    base_url: String,
    /// Shared HTTP client used for all `GET /v1/{reference}`
    /// requests.
    client: reqwest::Client,
    /// Set when the user requests exit (e.g. `q` / `Esc` / `Ctrl-C`).
    should_quit: bool,

    /// Flattened topology tree built by walking references from
    /// `"root"`.
    tree: Vec<TreeNode>,
    /// Currently selected index into `visible_indices()`.
    selected: usize,
    /// Detail payload for the selected node (usually served from
    /// `node_cache`).
    detail: Option<NodePayload>,
    /// Error string for the detail pane when fetching/parsing the
    /// selected node fails.
    detail_error: Option<String>,

    /// Timestamp string for the last successful refresh (local time).
    last_refresh: String,
    /// Top-level connection/refresh error surfaced in the header.
    error: Option<String>,

    /// Whether to show system/infrastructure procs (toggled via `s`).
    show_system_procs: bool,

    /// Cache of fetched node payloads, keyed by reference string.
    node_cache: HashMap<String, NodePayload>,
}

/// Result of handling a key event.
enum KeyResult {
    /// Nothing changed.
    None,
    /// Selection or expand/collapse changed; update detail from cache.
    DetailChanged,
    /// A filter/view setting changed; full tree refresh needed.
    NeedsRefresh,
}

impl App {
    /// Construct a new TUI app instance targeting the given admin
    /// server address.
    ///
    /// `addr` is the host:port pair (e.g. `127.0.0.1:8080`); the HTTP
    /// base URL is derived from it.
    fn new(addr: &str) -> Self {
        Self {
            base_url: format!("http://{}", addr),
            client: reqwest::Client::new(),
            should_quit: false,
            tree: Vec::new(),
            selected: 0,
            detail: None,
            detail_error: None,
            last_refresh: String::new(),
            error: None,
            show_system_procs: false,
            node_cache: HashMap::new(),
        }
    }

    /// Return the indices of `self.tree` that are currently visible.
    ///
    /// Visibility is determined by expansion state: any node whose
    /// ancestor is collapsed is hidden. The returned indices are in
    /// on-screen order (top-to-bottom).
    fn visible_indices(&self) -> Vec<usize> {
        let mut visible = Vec::new();
        let mut skip_below: Option<usize> = None;

        for (i, node) in self.tree.iter().enumerate() {
            if let Some(depth) = skip_below {
                if node.depth > depth {
                    continue;
                }
                skip_below = None;
            }
            visible.push(i);
            if node.has_children && !node.expanded {
                skip_below = Some(node.depth);
            }
        }
        visible
    }

    /// Map the current on-screen selection (`self.selected`) to a
    /// concrete index in `self.tree`.
    ///
    /// Returns `None` if the selection is out of range (e.g. the tree
    /// is empty).
    fn selected_tree_index(&self) -> Option<usize> {
        let visible = self.visible_indices();
        visible.get(self.selected).copied()
    }

    /// Fetch a single node payload from the admin API.
    ///
    /// `reference` is the opaque identifier used by the server (e.g.
    /// `"root"`, a `ProcId` string, or an `ActorId` string). The
    /// reference is URL-encoded and requested from `GET
    /// /v1/{reference}`. Returns a parsed `NodePayload` on success,
    /// or a human-readable error string on failure.
    async fn fetch_node(&self, reference: &str) -> Result<NodePayload, String> {
        let url = format!("{}/v1/{}", self.base_url, urlencoding::encode(reference));
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;
        if resp.status().is_success() {
            resp.json::<NodePayload>()
                .await
                .map_err(|e| format!("Parse error: {}", e))
        } else {
            Err(format!("HTTP {}", resp.status()))
        }
    }

    /// Refresh the in-memory topology model by re-walking the
    /// reference graph from `"root"`.
    ///
    /// Preserves expansion state (and tries to preserve the current
    /// selection) across rebuilds, updates the node cache, and then
    /// refreshes the detail pane for the currently selected row.
    async fn refresh(&mut self) {
        self.error = None;

        // Save expanded state before rebuilding.
        let expanded_keys: HashSet<String> = self
            .tree
            .iter()
            .filter(|n| n.expanded)
            .map(|n| n.reference.clone())
            .collect();

        // Save current selection's reference to restore position.
        let selected_key = self
            .selected_tree_index()
            .and_then(|idx| self.tree.get(idx))
            .map(|n| n.reference.clone());

        // Fetch root.
        let root = match self.fetch_node("root").await {
            Ok(payload) => payload,
            Err(e) => {
                self.error = Some(format!("Failed to connect: {}", e));
                return;
            }
        };

        let mut tree = Vec::new();
        let mut cache = HashMap::new();
        cache.insert("root".to_string(), root.clone());

        // Build tree recursively from root's children (skip root node
        // itself — hosts stay at depth 0, matching the old layout).
        let mut root_children = root.children.clone();
        root_children.sort_by(|a, b| natural_ref_cmp(a, b));
        for child_ref in &root_children {
            self.build_subtree(&mut tree, &mut cache, child_ref, 0, &expanded_keys)
                .await;
        }

        self.tree = tree;
        self.node_cache = cache;
        self.last_refresh = chrono::Local::now().format("%H:%M:%S").to_string();

        // Restore selection position.
        let visible = self.visible_indices();
        if let Some(ref key) = selected_key {
            if let Some(pos) = visible.iter().position(|&idx| {
                self.tree
                    .get(idx)
                    .map(|n| n.reference == *key)
                    .unwrap_or(false)
            }) {
                self.selected = pos;
            }
        }
        // Clamp selection.
        if !visible.is_empty() && self.selected >= visible.len() {
            self.selected = visible.len() - 1;
        }

        // Update detail from cache for current selection.
        self.update_selected_detail().await;
    }

    /// Recursively expand a reference into a flattened `TreeNode`
    /// list.
    ///
    /// Fetches `reference` from the admin API, appends a
    /// corresponding `TreeNode` to `tree`, caches the full
    /// `NodePayload` in `cache`, and (if the node is marked expanded
    /// in `expanded_keys`) recurses into its children until
    /// `MAX_TREE_DEPTH` is reached.
    ///
    /// Notes:
    /// - Uses `cache` as a visited set to avoid cycles / duplicate
    ///   display when the same reference appears under multiple parents
    ///   (only the first occurrence is shown).
    /// - Applies view filtering (e.g. hide `is_system` procs unless
    ///   enabled) before inserting.
    /// - Returns a boxed future so callers can build the tree with
    ///   async recursion without an explicit `async fn` recursive
    ///   signature.
    fn build_subtree<'a>(
        &'a self,
        tree: &'a mut Vec<TreeNode>,
        cache: &'a mut HashMap<String, NodePayload>,
        reference: &'a str,
        depth: usize,
        expanded_keys: &'a HashSet<String>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            // Depth guard.
            if depth >= MAX_TREE_DEPTH {
                return;
            }

            // If already visited, skip silently. A node can appear in
            // multiple parents' children lists (e.g. sieve actors); we
            // only display it under the first parent encountered.
            if cache.contains_key(reference) {
                return;
            }

            let payload = match self.fetch_node(reference).await {
                Ok(p) => p,
                Err(_) => return,
            };

            // Filter: skip system procs when hidden.
            if let NodeProperties::Proc { is_system, .. } = payload.properties {
                if !self.show_system_procs && is_system {
                    return;
                }
            }

            let label = derive_label(&payload);
            let node_type = NodeType::from_properties(&payload.properties);
            let has_children = !payload.children.is_empty();

            tree.push(TreeNode {
                label,
                depth,
                reference: reference.to_string(),
                node_type,
                expanded: expanded_keys.contains(reference),
                has_children,
            });

            cache.insert(reference.to_string(), payload.clone());

            // Only recurse into children when the node is expanded.
            // Collapsed nodes still show the fold arrow via has_children.
            if expanded_keys.contains(reference) {
                let mut children = payload.children.clone();
                children.sort_by(|a, b| natural_ref_cmp(a, b));

                if matches!(payload.properties, NodeProperties::Proc { .. }) {
                    for child_ref in &children {
                        if expanded_keys.contains(child_ref.as_str()) {
                            self.build_subtree(tree, cache, child_ref, depth + 1, expanded_keys)
                                .await;
                        } else {
                            let label = derive_label_from_ref(child_ref);
                            tree.push(TreeNode {
                                label,
                                depth: depth + 1,
                                reference: child_ref.clone(),
                                node_type: NodeType::Actor,
                                expanded: false,
                                has_children: true,
                            });
                        }
                    }
                } else {
                    for child_ref in &children {
                        self.build_subtree(tree, cache, child_ref, depth + 1, expanded_keys)
                            .await;
                    }
                }
            }
        })
    }

    /// Update the right-hand detail pane for the currently selected
    /// row.
    ///
    /// Looks up the selected node’s reference and populates
    /// `self.detail` from `node_cache` when available; otherwise
    /// fetches the payload from the admin API and caches it. On fetch
    /// failure, clears `detail` and records a human-readable error in
    /// `detail_error` so the UI can display it.
    async fn update_selected_detail(&mut self) {
        self.detail = None;
        self.detail_error = None;

        if let Some(idx) = self.selected_tree_index() {
            if let Some(node) = self.tree.get(idx) {
                let reference = node.reference.clone();
                if let Some(payload) = self.node_cache.get(&reference) {
                    self.detail = Some(payload.clone());
                } else {
                    match self.fetch_node(&reference).await {
                        Ok(payload) => {
                            self.detail = Some(payload.clone());
                            if let Some(node) = self.tree.get_mut(idx) {
                                node.has_children = !payload.children.is_empty();
                            }
                            self.node_cache.insert(reference, payload);
                        }
                        Err(e) => {
                            self.detail_error = Some(format!("Fetch failed: {}", e));
                        }
                    }
                }
            }
        }
    }

    /// Handle a single keypress and update in-memory UI state.
    ///
    /// Returns a `KeyResult` describing whether only the
    /// selection/expand state changed (so the detail pane should be
    /// refreshed) or whether a full topology refresh is required
    /// (e.g. after expanding nodes or toggling system-proc
    /// visibility).
    fn on_key(&mut self, key: KeyEvent) -> KeyResult {
        let visible = self.visible_indices();
        let vis_len = visible.len();

        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                self.should_quit = true;
                KeyResult::None
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                KeyResult::None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.selected > 0 {
                    self.selected -= 1;
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected + 1 < vis_len {
                    self.selected += 1;
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Home | KeyCode::Char('g') => {
                if self.selected != 0 {
                    self.selected = 0;
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::End | KeyCode::Char('G') => {
                if vis_len > 0 && self.selected != vis_len - 1 {
                    self.selected = vis_len - 1;
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Tab => {
                // Expand selected node; triggers refresh to fetch children.
                if let Some(&tree_idx) = visible.get(self.selected) {
                    if let Some(node) = self.tree.get_mut(tree_idx) {
                        if node.has_children && !node.expanded {
                            node.expanded = true;
                            return KeyResult::NeedsRefresh;
                        }
                    }
                }
                KeyResult::None
            }
            KeyCode::BackTab => {
                // Collapse selected node
                if let Some(&tree_idx) = visible.get(self.selected) {
                    if let Some(node) = self.tree.get_mut(tree_idx) {
                        if node.has_children && node.expanded {
                            node.expanded = false;
                            return KeyResult::DetailChanged;
                        }
                    }
                }
                KeyResult::None
            }
            KeyCode::Char('e') => {
                // Expand all; triggers refresh to fetch all children.
                for node in &mut self.tree {
                    if node.has_children {
                        node.expanded = true;
                    }
                }
                KeyResult::NeedsRefresh
            }
            KeyCode::Char('c') => {
                // Collapse all (plain 'c'; Ctrl+C is handled above)
                for node in &mut self.tree {
                    node.expanded = false;
                }
                KeyResult::DetailChanged
            }
            KeyCode::Char('s') => {
                // Toggle system proc visibility
                self.show_system_procs = !self.show_system_procs;
                KeyResult::NeedsRefresh
            }
            _ => KeyResult::None,
        }
    }
}

// Helpers

/// Derive a human-friendly label for a resolved node payload.
///
/// Kept as a free function (rather than an inherent `NodePayload`
/// method) because `NodePayload` lives in
/// `hyperactor_mesh::mesh_admin`; adding an extension trait here
/// would be more ceremony than it's worth for a small formatting
/// helper.
///
/// Uses `NodeProperties` to format a concise label for the tree view:
/// roots show host counts, hosts show proc counts, procs show actor
/// counts, and actors are rendered as `name[pid]` when the identity
/// parses as an `ActorId`.
fn derive_label(payload: &NodePayload) -> String {
    match &payload.properties {
        NodeProperties::Root { num_hosts } => format!("Mesh Root ({} hosts)", num_hosts),
        NodeProperties::Host { addr, num_procs } => format!("{}  ({} procs)", addr, num_procs),
        NodeProperties::Proc {
            proc_name,
            num_actors,
            ..
        } => {
            let short = ProcId::from_str(proc_name)
                .ok()
                .and_then(|pid| pid.name().cloned())
                .unwrap_or_else(|| proc_name.clone());
            format!("{}  ({} actors)", short, num_actors)
        }
        NodeProperties::Actor { .. } => match ActorId::from_str(&payload.identity) {
            Ok(actor_id) => format!("{}[{}]", actor_id.name(), actor_id.pid()),
            Err(_) => payload.identity.clone(),
        },
        NodeProperties::Error { code, message } => {
            format!("[error] {}: {}", code, message)
        }
    }
}

/// Derive a display label from an opaque reference string without
/// fetching.
///
/// If the reference parses as an `ActorId`, format it as `name[pid]`;
/// otherwise fall back to showing the raw reference.
fn derive_label_from_ref(reference: &str) -> String {
    match ActorId::from_str(reference) {
        Ok(actor_id) => format!("{}[{}]", actor_id.name(), actor_id.pid()),
        Err(_) => reference.to_string(),
    }
}

/// Compare reference strings using a "natural" order for trailing
/// `[N]` indices.
///
/// If both strings end with a bracketed numeric suffix (e.g.
/// `foo[2]`), compares their non-index prefixes lexicographically and
/// the numeric suffixes numerically so `...[2]` sorts before
/// `...[10]`. If either string lacks a trailing numeric index, falls
/// back to plain lexicographic comparison.
fn natural_ref_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (extract_trailing_index(a), extract_trailing_index(b)) {
        (Some((prefix_a, idx_a)), Some((prefix_b, idx_b))) => {
            prefix_a.cmp(prefix_b).then(idx_a.cmp(&idx_b))
        }
        _ => a.cmp(b),
    }
}

/// Parse a trailing bracketed numeric index from a reference string.
///
/// Returns `(prefix, N)` for strings ending in `[N]` (e.g.
/// `foo[12]`), where `prefix` is everything before the final `[` and
/// `N` is the parsed `u64`. Returns `None` if the string does not end
/// in a well-formed numeric index.
fn extract_trailing_index(s: &str) -> Option<(&str, u64)> {
    let s = s.strip_suffix(']')?;
    let bracket = s.rfind('[')?;
    let num: u64 = s[bracket + 1..].parse().ok()?;
    Some((&s[..bracket], num))
}

/// Produce a compact, human-readable summary string for a recorded
/// event.
///
/// Prefers common message-like fields (`message`, then `msg`),
/// otherwise renders a useful hint such as `handler: ...`. As a
/// fallback, formats up to three key/value pairs from the event
/// fields (using `format_value`) to keep the TUI line short; if
/// nothing matches, falls back to the event `name`.
fn format_event_summary(name: &str, fields: &Value) -> String {
    if let Some(obj) = fields.as_object() {
        if let Some(msg) = obj.get("message").and_then(|v| v.as_str()) {
            return msg.to_string();
        }
        if let Some(msg) = obj.get("msg").and_then(|v| v.as_str()) {
            return msg.to_string();
        }
        if let Some(handler) = obj.get("handler").and_then(|v| v.as_str()) {
            return format!("handler: {}", handler);
        }
        if !obj.is_empty() {
            let summary: Vec<String> = obj
                .iter()
                .take(3)
                .map(|(k, v)| format!("{}={}", k, format_value(v)))
                .collect();
            if !summary.is_empty() {
                return summary.join(" ");
            }
        }
    }
    name.to_string()
}

/// Format a JSON value into a short, single-line representation
/// suitable for the TUI.
///
/// Strings/numbers/bools render as-is; `null` renders as `"null"`.
/// Arrays and objects are summarized by their length/field count
/// (e.g. `"[3]"`, `"{5}"`) to avoid dumping large payloads into the
/// event list.
fn format_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(arr) => format!("[{}]", arr.len()),
        Value::Object(obj) => format!("{{{}}}", obj.len()),
    }
}

/// Convert an ISO 8601 UTC timestamp (e.g.
/// "2026-02-11T19:11:01.265Z") to a local-timezone HH:MM:SS string.
/// Falls back to extracting the raw UTC time portion if parsing
/// fails.
fn format_local_time(timestamp: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|_| timestamp.get(11..19).unwrap_or(timestamp).to_string())
}

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

/// Restore the terminal back to normal “shell mode”.
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

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    if !io::stdout().is_terminal() {
        eprintln!("This TUI requires a real terminal.");
        return Ok(());
    }

    // Show an indicatif spinner on stderr while fetching initial data.
    // This runs before the alternate screen so it's visible as a normal
    // terminal line.
    let mut app = App::new(&args.addr);
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid template"),
    );
    spinner.set_message(format!(
        "🦋 Monarch Admin — Connecting to {} ...",
        app.base_url
    ));
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

/// Drive the main event loop for the admin TUI.
///
/// Periodically refreshes topology from the admin API, renders the UI
/// each tick, and processes keyboard input until the user exits.
async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    args: &Args,
    mut app: App,
) -> io::Result<()> {
    let mut refresh_interval = tokio::time::interval(Duration::from_millis(args.refresh_ms));
    let mut events = EventStream::new();

    loop {
        terminal.draw(|frame| ui(frame, &app))?;

        tokio::select! {
            _ = refresh_interval.tick() => {
                app.refresh().await;
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) => {
                        match app.on_key(key) {
                            KeyResult::DetailChanged => {
                                app.update_selected_detail().await;
                            }
                            KeyResult::NeedsRefresh => {
                                app.refresh().await;
                            }
                            KeyResult::None => {}
                        }
                    }
                    Some(Ok(Event::Resize(_, _))) => {}
                    _ => {}
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

// UI rendering

/// Render a full frame of the TUI.
///
/// Splits the screen into header/body/footer regions and delegates to
/// the corresponding render helpers.
fn ui(frame: &mut ratatui::Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(2),
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);
    render_body(frame, chunks[1], app);
    render_footer(frame, chunks[2]);
}

/// Render the top status/header bar.
///
/// Shows connection status (or the last refresh time) and reflects
/// current view toggles like whether system procs are visible.
fn render_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let status = if let Some(err) = &app.error {
        format!("ERROR: {}", err)
    } else {
        let sys = if app.show_system_procs {
            " | system procs: shown"
        } else {
            ""
        };
        format!(
            "Connected to {} | Last refresh: {}{}",
            app.base_url, app.last_refresh, sys
        )
    };

    let header = Paragraph::new(vec![
        Line::from(Span::styled(
            "🦋 Monarch Admin",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(status, Style::default().fg(Color::Gray))),
    ])
    .block(Block::default().borders(Borders::BOTTOM));

    frame.render_widget(header, area);
}

/// Render the main body of the UI.
///
/// Splits the screen into a left topology pane and a right detail
/// pane, and renders each using the current application state.
fn render_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    render_topology_tree(frame, chunks[0], app);
    render_detail_pane(frame, chunks[1], app);
}

/// Render the topology tree (left pane).
///
/// Uses the flattened `app.tree` plus `visible_indices()` to display
/// only nodes not hidden by collapsed ancestors. Each row includes
/// indentation/connectors, an expand/collapse glyph for nodes with
/// children, and color-coding by `NodeType`, with the selected row
/// highlighted.
fn render_topology_tree(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let visible = app.visible_indices();

    let items: Vec<ListItem> = visible
        .iter()
        .enumerate()
        .map(|(vis_idx, &tree_idx)| {
            let node = &app.tree[tree_idx];
            let indent = "  ".repeat(node.depth);

            // Tree connector
            let connector = if node.depth == 0 {
                ""
            } else {
                // Check if there's a sibling after this node (at the same depth)
                let has_sibling = {
                    let mut found = false;
                    for &next_idx in visible.iter().skip(vis_idx + 1) {
                        let next = &app.tree[next_idx];
                        if next.depth < node.depth {
                            break;
                        }
                        if next.depth == node.depth {
                            found = true;
                            break;
                        }
                    }
                    found
                };
                if has_sibling { "├─ " } else { "└─ " }
            };

            // Fold indicator for expandable nodes
            let fold = if node.has_children {
                if node.expanded { "▼ " } else { "▶ " }
            } else {
                "  "
            };

            let style = if vis_idx == app.selected {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                match node.node_type {
                    NodeType::Root => Style::default().fg(Color::Magenta),
                    NodeType::Host => Style::default().fg(Color::Cyan),
                    NodeType::Proc => Style::default().fg(Color::Green),
                    NodeType::Actor => Style::default().fg(Color::White),
                }
            };

            ListItem::new(Line::from(Span::styled(
                format!("{}{}{}{}", indent, connector, fold, node.label),
                style,
            )))
        })
        .collect();

    let block = Block::default()
        .title("Topology")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default());
    let mut list_state = ListState::default().with_selected(Some(app.selected));
    frame.render_stateful_widget(list, area, &mut list_state);
}

/// Render the contextual details pane (right side).
///
/// If a `NodePayload` for the current selection is available in
/// `app.detail`, dispatches to `render_node_detail` to show a
/// type-specific view (root/host/proc/actor). Otherwise, shows either
/// the last fetch error (`app.detail_error`) or a neutral "select a
/// node" placeholder message.
fn render_detail_pane(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    match &app.detail {
        Some(payload) => render_node_detail(frame, area, payload),
        None => {
            let msg = app
                .detail_error
                .as_deref()
                .unwrap_or("Select a node to view details");
            let color = if app.detail_error.is_some() {
                Color::Red
            } else {
                Color::Gray
            };
            let block = Block::default()
                .title("Details")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Gray));
            let p = Paragraph::new(Span::styled(msg, Style::default().fg(color))).block(block);
            frame.render_widget(p, area);
        }
    }
}

/// Render the details view for a resolved node.
///
/// This is the main dispatcher for the right-hand pane: it matches on
/// `payload.properties` and forwards to the appropriate renderer
/// (`render_root_detail`, `render_host_detail`, `render_proc_detail`,
/// or `render_actor_detail`) with the relevant fields extracted from
/// the payload.
fn render_node_detail(frame: &mut ratatui::Frame<'_>, area: Rect, payload: &NodePayload) {
    match &payload.properties {
        NodeProperties::Root { num_hosts } => {
            render_root_detail(frame, area, payload, *num_hosts);
        }
        NodeProperties::Host { addr, num_procs } => {
            render_host_detail(frame, area, payload, addr, *num_procs);
        }
        NodeProperties::Proc {
            proc_name,
            num_actors,
            ..
        } => {
            render_proc_detail(frame, area, payload, proc_name, *num_actors);
        }
        NodeProperties::Actor {
            actor_status,
            actor_type,
            messages_processed,
            created_at,
            last_message_handler,
            total_processing_time_us,
            flight_recorder,
        } => {
            render_actor_detail(
                frame,
                area,
                payload,
                actor_status,
                actor_type,
                *messages_processed,
                created_at,
                last_message_handler.as_deref(),
                *total_processing_time_us,
                flight_recorder.as_deref(),
            );
        }
        NodeProperties::Error { code, message } => {
            let text = format!("Error: {} — {}", code, message);
            let paragraph = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Error"))
                .wrap(Wrap { trim: true });
            frame.render_widget(paragraph, area);
        }
    }
}

/// Render the right-pane detail view for the mesh root node.
///
/// Shows a simple summary (host count) and then lists the root’s
/// immediate children (host references) so the user can quickly see
/// which hosts are currently registered under the mesh.
fn render_root_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    num_hosts: usize,
) {
    let block = Block::default()
        .title("Root Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Hosts: ", Style::default().fg(Color::Gray)),
            Span::raw(num_hosts.to_string()),
        ]),
        Line::default(),
    ];
    for child in &payload.children {
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(child, Style::default().fg(Color::Cyan)),
        ]));
    }

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

/// Render the right-pane detail view for a host node.
///
/// Displays the host's address and proc count, then lists the host’s
/// proc children using a shortened proc name for readability.
fn render_host_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    addr: &str,
    num_procs: usize,
) {
    let block = Block::default()
        .title("Host Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Address: ", Style::default().fg(Color::Gray)),
            Span::raw(addr),
        ]),
        Line::from(vec![
            Span::styled("Procs: ", Style::default().fg(Color::Gray)),
            Span::raw(num_procs.to_string()),
        ]),
        Line::default(),
    ];
    for child in &payload.children {
        let short = ProcId::from_str(child)
            .ok()
            .and_then(|pid| pid.name().cloned())
            .unwrap_or_else(|| child.clone());
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(short, Style::default().fg(Color::Green)),
        ]));
    }

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

/// Render the right-pane detail view for a proc node.
///
/// Shows the proc's full name and actor count, then lists up to the
/// first 50 child actor references (with an elision line if there are
/// more) to keep the UI responsive and the pane readable.
fn render_proc_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    proc_name: &str,
    num_actors: usize,
) {
    let block = Block::default()
        .title("Proc Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Name: ", Style::default().fg(Color::Gray)),
            Span::raw(proc_name),
        ]),
        Line::from(vec![
            Span::styled("Actors: ", Style::default().fg(Color::Gray)),
            Span::raw(num_actors.to_string()),
        ]),
        Line::default(),
    ];
    for (i, actor) in payload.children.iter().enumerate() {
        if i >= 50 {
            lines.push(Line::from(Span::styled(
                format!("  ... and {} more", payload.children.len() - 50),
                Style::default().fg(Color::DarkGray),
            )));
            break;
        }
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::raw(actor),
        ]));
    }

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

/// Render the right-pane detail view for an actor node, including
/// flight-recorder events.
///
/// The top section summarizes actor status/type, message/processing
/// stats, creation time, last handler, and child count; the bottom
/// section parses the optional flight-recorder JSON and displays a
/// compact, timestamped list of recent events.
#[allow(clippy::too_many_arguments)]
fn render_actor_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    actor_status: &str,
    actor_type: &str,
    messages_processed: u64,
    created_at: &str,
    last_message_handler: Option<&str>,
    total_processing_time_us: u64,
    flight_recorder_json: Option<&str>,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(5)])
        .split(area);

    // Actor info
    let info_block = Block::default()
        .title("Actor Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let info = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Status: ", Style::default().fg(Color::Gray)),
            Span::styled(
                actor_status,
                Style::default().fg(if actor_status == "Running" {
                    Color::Green
                } else {
                    Color::Yellow
                }),
            ),
        ]),
        Line::from(vec![
            Span::styled("Type: ", Style::default().fg(Color::Gray)),
            Span::raw(actor_type),
        ]),
        Line::from(vec![
            Span::styled("Messages: ", Style::default().fg(Color::Gray)),
            Span::raw(messages_processed.to_string()),
        ]),
        Line::from(vec![
            Span::styled("Processing time: ", Style::default().fg(Color::Gray)),
            Span::raw(
                humantime::format_duration(std::time::Duration::from_micros(
                    total_processing_time_us,
                ))
                .to_string(),
            ),
        ]),
        Line::from(vec![
            Span::styled("Created: ", Style::default().fg(Color::Gray)),
            Span::raw(created_at),
        ]),
        Line::from(vec![
            Span::styled("Last handler: ", Style::default().fg(Color::Gray)),
            Span::raw(last_message_handler.unwrap_or("-")),
        ]),
        Line::from(vec![
            Span::styled("Children: ", Style::default().fg(Color::Gray)),
            Span::raw(payload.children.len().to_string()),
        ]),
    ])
    .block(info_block);
    frame.render_widget(info, chunks[0]);

    // Flight recorder
    let recorder_block = Block::default()
        .title("Flight Recorder")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let recorded_events: Vec<RecordedEvent> = flight_recorder_json
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();

    let events: Vec<Line> = if recorded_events.is_empty() {
        vec![Line::from(Span::styled(
            "No events",
            Style::default().fg(Color::Gray),
        ))]
    } else {
        recorded_events
            .iter()
            .take(20)
            .map(|event| {
                let level_color = match event.level.as_str() {
                    "ERROR" => Color::Red,
                    "WARN" => Color::Yellow,
                    "INFO" => Color::Green,
                    "DEBUG" => Color::Blue,
                    _ => Color::Gray,
                };
                Line::from(vec![
                    Span::styled(
                        format!("{} ", event.level.chars().next().unwrap_or('?')),
                        Style::default().fg(level_color),
                    ),
                    Span::styled(
                        format!("{} ", format_local_time(&event.timestamp)),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::raw(format_event_summary(&event.name, &event.fields)),
                ])
            })
            .collect()
    };

    let recorder = Paragraph::new(events)
        .block(recorder_block)
        .wrap(Wrap { trim: true });
    frame.render_widget(recorder, chunks[1]);
}

/// Render the bottom help bar showing the keyboard shortcuts.
///
/// This is a static hint line
/// (quit/navigation/expand-collapse/filter) separated from the main
/// UI with a top border so it reads like a persistent status/help
/// footer.
fn render_footer(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let help = "q: quit | j/k: navigate | g/G: top/bottom | Tab/Shift-Tab: expand/collapse | e/c: expand/collapse all | s: system procs";
    let footer = Paragraph::new(help)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, area);
}
