/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::io;

use crossterm::event::Event;
use crossterm::event::EventStream;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyModifiers;
use futures::StreamExt;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::text::Line;
use ratatui::text::Span;
use tokio::sync::oneshot;

use crate::ActiveJob;
use crate::ActiveJobEvent;
use crate::ColorScheme;
use crate::Cursor;
use crate::FetchState;
use crate::KeyResult;
use crate::LangName;
use crate::NodeType;
use crate::Theme;
use crate::ThemeName;
use crate::TreeNode;
use crate::VisibleRows;
use crate::build_tree_node;
use crate::collapse_all;
use crate::collect_expanded_refs;
use crate::collect_failed_refs;
use crate::collect_refs;
use crate::derive_label;
use crate::diagnostics::run_diagnostics;
use crate::fetch_with_join;
use crate::find_at_depth_from_root_mut;
use crate::flatten_tree;
use crate::get_cached_payload;
use crate::is_failed_node;
use crate::is_stopped_node;
use crate::is_system_node;
use crate::overlay::Overlay;
use crate::render::ui;
use crate::sorted_children;
use crate::timeouts::TuiTimeoutPolicy;

// Application state

/// Runtime state for the admin TUI.
///
/// `App` owns the HTTP client, the currently materialized topology
/// tree, selection state, and a small cache of fetched
/// [`NodePayload`]s so navigation/detail rendering can be responsive
/// without re-fetching every node on every keypress.
pub(crate) struct App {
    /// Base URL for the admin server (e.g. `http://127.0.0.1:8080`).
    pub(crate) base_url: String,
    /// Shared HTTP client used for all `GET /v1/{reference}`
    /// requests.
    pub(crate) client: reqwest::Client,
    /// Central timeout policy (TP-*). See `timeouts.rs`.
    pub(crate) policy: TuiTimeoutPolicy,
    /// Set when the user requests exit (e.g. `q` / `Esc` / `Ctrl-C`).
    pub(crate) should_quit: bool,

    /// The topology tree rooted at the synthetic "root" node.
    ///
    /// `None` before the first successful refresh, `Some(tree)` after.
    /// The root node itself is not rendered (its children appear at
    /// depth 0).
    pub(crate) tree: Option<TreeNode>,
    /// Navigation cursor over visible tree indices.
    pub(crate) cursor: Cursor,
    /// Scroll offset for topology tree (top visible row).
    pub(crate) tree_scroll_offset: usize,
    /// Height of the topology tree viewport in rows (updated during
    /// rendering).
    pub(crate) tree_viewport_height: usize,
    /// Detail payload for the selected node (usually served from
    /// `node_cache`).
    pub(crate) detail: Option<NodePayload>,
    /// Error string for the detail pane when fetching/parsing the
    /// selected node fails.
    pub(crate) detail_error: Option<String>,

    /// Human-readable refresh interval (e.g. "1s", "5s").
    pub(crate) refresh_interval_label: String,
    /// Top-level connection/refresh error surfaced in the header.
    pub(crate) error: Option<String>,

    /// Whether to show system/infrastructure procs and actors
    /// (toggled via `s`).
    pub(crate) show_system: bool,

    /// Whether to show stopped actors (toggled via `h`).
    /// Hidden by default so the tree focuses on live actors.
    /// TUI-4: failed nodes always visible.
    pub(crate) show_stopped: bool,

    /// Fetch cache with generation-based staleness.
    pub(crate) fetch_cache: HashMap<NodeRef, FetchState<NodePayload>>,
    /// Current refresh generation for cache invalidation.
    pub(crate) refresh_gen: u64,
    /// Monotonic sequence counter for timestamp ordering.
    pub(crate) seq_counter: u64,

    /// Visual presentation (colors + labels).
    pub(crate) theme: Theme,
    /// Active theme name (for display in header).
    pub(crate) theme_name: ThemeName,
    /// Active language (for display in header).
    pub(crate) lang_name: LangName,

    /// The running or completed overlay-producing async job (TUI-21).
    /// `None` iff `overlay` is also `None`.
    pub(crate) active_job: Option<ActiveJob>,
    /// Active overlay (py-spy, config, or diagnostics content).
    /// When `Some`, the detail pane renders the overlay instead of
    /// node details. Dismissed with Esc, scrolled with j/k.
    pub(crate) overlay: Option<Overlay>,
}

impl App {
    /// Construct a new TUI app instance targeting the given admin
    /// server address.
    ///
    /// `base_url` should include the scheme (e.g. `http://host:port`
    /// or `https://host:port`).
    pub(crate) fn new(
        base_url: String,
        client: reqwest::Client,
        theme_name: ThemeName,
        lang_name: LangName,
        policy: TuiTimeoutPolicy,
    ) -> Self {
        Self {
            base_url,
            client,
            policy,
            should_quit: false,
            tree: None,
            cursor: Cursor::new(0),
            tree_scroll_offset: 0,
            tree_viewport_height: 20, // Default, updated during rendering
            detail: None,
            detail_error: None,
            refresh_interval_label: String::new(),
            error: None,
            show_system: false,
            show_stopped: false,
            fetch_cache: HashMap::new(),
            refresh_gen: 0,
            seq_counter: 0,
            theme: Theme::new(theme_name, lang_name),
            theme_name,
            lang_name,
            active_job: None,
            overlay: None,
        }
    }

    // Controlled mutation API - single entry point for tree modifications.

    /// Replace the entire tree structure.
    pub(crate) fn set_tree(&mut self, tree: Option<TreeNode>) {
        self.tree = tree;
    }

    /// Mutate the tree via a closure.
    ///
    /// Provides safe mutable access to the tree. This is the only way
    /// to mutate the tree after initialization.
    pub(crate) fn mutate_tree<F>(&mut self, f: F)
    where
        F: FnOnce(&mut TreeNode),
    {
        if let Some(tree) = &mut self.tree {
            f(tree);
        }
    }

    /// Immutable tree accessor.
    pub(crate) fn tree(&self) -> Option<&TreeNode> {
        self.tree.as_ref()
    }

    /// Fetch a single node payload from the admin API.
    ///
    /// `reference` is the opaque identifier used by the server (e.g.
    /// `"root"`, a `ProcId` string, or an `ActorId` string). The
    /// reference is URL-encoded and requested from `GET
    /// /v1/{reference}`. Returns a parsed `NodePayload` on success,
    /// or a human-readable error string on failure.
    ///
    /// This method is the centralized fetch path with caching and
    /// generation-based staleness.
    /// - `force = true`: bypass cache (used in refresh).
    /// - Errors are always retried (preserve current behavior).
    /// - Stale entries (`generation < refresh_gen`) are refetched.
    pub(crate) async fn fetch_node_state(
        &mut self,
        reference: &NodeRef,
        force: bool,
    ) -> FetchState<NodePayload> {
        fetch_with_join(
            &self.client,
            &self.base_url,
            reference,
            &mut self.fetch_cache,
            self.refresh_gen,
            &mut self.seq_counter,
            force,
            self.policy
                .request_timeout(crate::timeouts::RequestOp::InteractiveFetch),
        )
        .await
    }

    /// Extract a payload from FetchState if Ready, otherwise None.
    pub(crate) fn get_cached_payload(&self, reference: &NodeRef) -> Option<&NodePayload> {
        get_cached_payload(&self.fetch_cache, reference)
    }

    /// Return the indices of `self.tree` that are currently visible.
    ///
    /// Visibility is determined by expansion state: any node whose
    /// ancestor is collapsed is hidden. The returned indices are in
    /// on-screen order (top-to-bottom).
    ///
    /// Purely derived from tree structure - no caching, correct by
    /// construction.
    pub(crate) fn visible_rows(&self) -> VisibleRows<'_> {
        match self.tree() {
            Some(root) => VisibleRows::new(flatten_tree(root)),
            None => VisibleRows::new(Vec::new()),
        }
    }

    /// Get the currently selected node's type and label.
    ///
    /// Returns `None` if no node is selected or tree is empty.
    pub(crate) fn current_selection(&self) -> Option<&TreeNode> {
        let rows = self.visible_rows();
        rows.get(&self.cursor).map(|row| row.node)
    }

    /// Get the currently selected node's reference.
    ///
    /// Returns `None` if the selection is out of range (e.g. the tree
    /// is empty).
    pub(crate) fn selected_reference(&self) -> Option<&NodeRef> {
        let rows = self.visible_rows();
        rows.get(&self.cursor).map(|row| &row.node.reference)
    }

    /// Refresh the in-memory topology model by re-walking the
    /// reference graph from `"root"`.
    ///
    /// Preserves expansion state (and tries to preserve the current
    /// selection) across rebuilds, updates the node cache, and then
    /// refreshes the detail pane for the currently selected row.
    pub(crate) async fn refresh(&mut self) {
        self.error = None;
        self.refresh_gen += 1;

        // Save expanded and failed state before rebuilding.
        // Track (reference, depth) pairs to handle dual appearances correctly.
        let mut expanded_keys = HashSet::new();
        let mut failed_keys = HashSet::new();
        if let Some(root) = self.tree() {
            for child in &root.children {
                collect_expanded_refs(child, 0, &mut expanded_keys);
                collect_failed_refs(child, 0, &mut failed_keys);
            }
        }

        // Save current selection's reference and depth.
        let rows = self.visible_rows();
        let prev_selection = rows
            .get(&self.cursor)
            .map(|row| (row.node.reference.clone(), row.depth));

        // Fetch root using centralized fetch with force=true.
        let root_ref = NodeRef::Root;
        let root_state = self.fetch_node_state(&root_ref, true).await;
        let root_payload = match root_state {
            FetchState::Ready { value, .. } => value,
            FetchState::Error { msg, .. } => {
                self.error = Some(format!("Failed to connect: {}", msg));
                return;
            }
            FetchState::Unknown => return,
        };

        // Path for cycle detection: tracks current path from root to
        // Node being built. Start with root in the path.
        let mut path = vec![NodeRef::Root];

        // Build tree recursively from root's children.
        let mut root_children = Vec::new();
        let sorted = sorted_children(&root_payload);

        for child_ref in &sorted {
            if let Some(child_node) = build_tree_node(
                &self.client,
                &self.base_url,
                self.show_system,
                self.show_stopped,
                &mut self.fetch_cache,
                &mut path,
                child_ref,
                0,
                &expanded_keys,
                &failed_keys,
                self.refresh_gen,
                &mut self.seq_counter,
                self.policy
                    .request_timeout(crate::timeouts::RequestOp::InteractiveFetch),
            )
            .await
            {
                root_children.push(child_node);
            }
        }

        // Create synthetic root node.
        self.set_tree(Some(TreeNode {
            reference: NodeRef::Root,
            label: "Root".to_string(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: !root_children.is_empty(),
            stopped: false,
            failed: false,
            is_system: false,
            children: root_children,
        }));

        // Prune stale cache entries (collect owned refs to avoid borrow issues).
        let live_refs: HashSet<NodeRef> = if let Some(root) = self.tree() {
            let mut refs = HashSet::new();
            collect_refs(root, &mut refs);
            refs.into_iter().cloned().collect()
        } else {
            HashSet::new()
        };
        self.fetch_cache
            .retain(|k, _| matches!(k, NodeRef::Root) || live_refs.contains(k));

        // Restore selection position.
        let rows = self.visible_rows();
        if let Some((prev_ref, prev_depth)) = prev_selection {
            // Try to match both reference and depth first.
            let depth_match = rows
                .as_slice()
                .iter()
                .position(|row| row.node.reference == prev_ref && row.depth == prev_depth);
            // Fall back to matching just reference.
            let any_match = rows
                .as_slice()
                .iter()
                .position(|row| row.node.reference == prev_ref);

            self.cursor.update_len(rows.len());
            if let Some(pos) = depth_match.or(any_match) {
                self.cursor.set_pos(pos);
            }
        } else {
            self.cursor.update_len(rows.len());
        }

        // Update detail from cache for current selection.
        self.update_selected_detail().await;
    }

    /// Lazily expand a single node by fetching its children.
    ///
    /// If the node's payload is already cached, uses it; otherwise
    /// fetches from the admin API. For Proc/Actor parents, children
    /// are inserted as placeholders (lazy fetching). For Root/Host
    /// parents, children are eagerly fetched.
    pub(crate) async fn expand_node(&mut self, reference: &NodeRef, depth: usize) -> bool {
        // Early check: bail if no tree.
        if self.tree.is_none() {
            return false;
        }

        // Fetch payload (releases tree borrow).
        let state = self.fetch_node_state(reference, false).await;
        let payload = match state {
            FetchState::Ready { value, .. } => value,
            FetchState::Error { msg, .. } => {
                self.error = Some(format!("Expand failed: {}", msg));
                return false;
            }
            FetchState::Unknown => return false,
        };

        // Build children from payload.
        let children = sorted_children(&payload);

        let is_proc_or_actor = matches!(
            payload.properties,
            NodeProperties::Proc { .. } | NodeProperties::Actor { .. }
        );

        // Extract system_children from the parent for lazy filtering.
        let system_children: HashSet<&NodeRef> = match &payload.properties {
            NodeProperties::Root {
                system_children, ..
            }
            | NodeProperties::Host {
                system_children, ..
            }
            | NodeProperties::Proc {
                system_children, ..
            } => system_children.iter().collect(),
            _ => HashSet::new(),
        };

        // Extract stopped_children from proc payloads for lazy
        // filtering/graying without per-child fetches.
        let (stopped_children, parent_is_poisoned): (HashSet<&NodeRef>, bool) =
            match &payload.properties {
                NodeProperties::Proc {
                    stopped_children,
                    is_poisoned,
                    ..
                } => (stopped_children.iter().collect(), *is_poisoned),
                _ => (HashSet::new(), false),
            };

        let mut child_nodes = Vec::new();
        for child_ref in &children {
            // Filter order: system first, then stopped.
            if !self.show_system && system_children.contains(child_ref) {
                continue;
            }

            let child_is_stopped = stopped_children.contains(child_ref);
            let child_is_system = system_children.contains(child_ref);

            // TUI-4: failed nodes always visible.
            // If the parent proc is poisoned, its stopped children may be
            // failed — don't filter them out (cache may be empty on first load).
            let child_is_failed = parent_is_poisoned
                || self
                    .get_cached_payload(child_ref)
                    .is_some_and(|c| is_failed_node(&c.properties));
            if !self.show_stopped && child_is_stopped && !child_is_failed {
                continue;
            }

            if is_proc_or_actor {
                // Lazy: use placeholder or cached payload.
                if let Some(cached) = self.get_cached_payload(child_ref) {
                    // Fallback: also check cached payload in case proc
                    // payload is stale.
                    if !self.show_stopped
                        && is_stopped_node(&cached.properties)
                        && !is_failed_node(&cached.properties)
                    {
                        continue;
                    }
                    let mut node = TreeNode::from_payload(child_ref.clone(), cached);
                    node.stopped = node.stopped || child_is_stopped;
                    node.is_system = node.is_system || child_is_system;
                    child_nodes.push(node);
                } else if child_is_stopped {
                    let mut node = TreeNode::placeholder_stopped(child_ref.clone());
                    node.is_system = child_is_system;
                    child_nodes.push(node);
                } else {
                    let mut node = TreeNode::placeholder(child_ref.clone());
                    node.is_system = child_is_system;
                    child_nodes.push(node);
                }
            } else {
                // Eager: fetch child using centralized fetch path.
                let child_state = self.fetch_node_state(child_ref, false).await;
                let child_payload = match child_state {
                    FetchState::Ready { value, .. } => Some(value),
                    FetchState::Error { msg, .. } => {
                        self.error = Some(format!("Child fetch failed for {}: {}", child_ref, msg));
                        None
                    }
                    FetchState::Unknown => None,
                };

                if let Some(cp) = child_payload {
                    // Apply system filtering for procs and actors.
                    if !self.show_system && is_system_node(&cp.properties) {
                        continue;
                    }
                    // TUI-4: failed nodes always visible.
                    if !self.show_stopped
                        && is_stopped_node(&cp.properties)
                        && !is_failed_node(&cp.properties)
                    {
                        continue;
                    }
                    child_nodes.push(TreeNode::from_payload(child_ref.clone(), &cp));
                }
            }
        }

        // Now update the node with the new data.
        if let Some(root) = &mut self.tree
            && let Some(node) = find_at_depth_from_root_mut(root, reference, depth)
        {
            // Skip if already expanded with children
            if node.expanded && !node.children.is_empty() {
                return false;
            }

            node.label = derive_label(&payload);
            node.has_children = !payload.children.is_empty();
            node.fetched = true;
            node.node_type = NodeType::from_properties(&payload.properties);
            node.stopped = is_stopped_node(&payload.properties);
            node.failed =
                is_failed_node(&payload.properties) || child_nodes.iter().any(|c| c.failed);
            node.is_system = is_system_node(&payload.properties);
            node.children = child_nodes;
            node.expanded = true;
            // Keep scroll offset stable during expansion - the cursor
            // position and view should remain unchanged.
            return true;
        }

        false
    }

    /// Update the right-hand detail pane for the currently selected
    /// row.
    ///
    /// Looks up the selected node's reference and populates
    /// `self.detail` from `fetch_cache` when available; otherwise
    /// fetches the payload from the admin API and caches it. On fetch
    /// failure, clears `detail` and records a human-readable error in
    /// `detail_error` so the UI can display it.
    pub(crate) async fn update_selected_detail(&mut self) {
        self.detail = None;
        self.detail_error = None;

        // Get reference first (releases tree borrow).
        let reference = self.selected_reference().cloned();

        if let Some(node_ref) = reference {
            let state = self.fetch_node_state(&node_ref, false).await;
            match state {
                FetchState::Ready { value, .. } => {
                    self.detail = Some(value);
                }
                FetchState::Error { msg, .. } => {
                    self.detail_error = Some(format!("Fetch failed: {}", msg));
                }
                FetchState::Unknown => {}
            }
        }
    }

    /// Adjust scroll offset to ensure cursor remains visible within
    /// the viewport.
    ///
    /// After Ctrl+L sets an explicit offset to position the selected
    /// item at the top, this method preserves that positioning during
    /// navigation, only adjusting when the cursor would move
    /// off-screen.
    pub(crate) fn ensure_cursor_visible(&mut self) {
        let pos = self.cursor.pos();
        if pos < self.tree_scroll_offset {
            // Cursor moved above visible area, scroll up to show it at
            // top.
            self.tree_scroll_offset = pos;
        } else if pos >= self.tree_scroll_offset + self.tree_viewport_height {
            // Cursor moved below visible area, scroll down to show it
            // near bottom.
            self.tree_scroll_offset = pos.saturating_sub(self.tree_viewport_height - 1);
        }
        // Otherwise, cursor is visible - keep offset unchanged.
    }

    /// Return the proc reference to use for a py-spy request given the
    /// current cursor position.
    ///
    /// - Proc selected → proc's own reference.
    /// - Actor selected → owning proc from `detail.parent`.
    /// - Root/Host selected → `None` (PY-4).
    pub(crate) fn pyspy_proc_ref(&self) -> Option<hyperactor::reference::ProcId> {
        let rows = self.visible_rows();
        let row = rows.get(&self.cursor)?;
        match (&row.node.node_type, &row.node.reference) {
            (NodeType::Proc, NodeRef::Proc(proc_id)) => Some(proc_id.clone()),
            (NodeType::Actor, _) => self.detail.as_ref().and_then(|p| match &p.parent {
                Some(NodeRef::Proc(proc_id)) => Some(proc_id.clone()),
                _ => None,
            }),
            _ => None,
        }
    }

    /// Set the active job and build its overlay. TUI-21: both fields
    /// are always set together. Replacing a prior variant drops its
    /// receiver, cancelling any in-flight async work (PY-2).
    pub(crate) fn set_job(&mut self, job: ActiveJob) {
        self.active_job = Some(job);
        self.overlay = None; // clear stale overlay so scroll starts at 0
        self.rebuild_overlay();
    }

    /// Dismiss the active job and its overlay. TUI-21: both fields
    /// are always cleared together.
    pub(crate) fn dismiss_job(&mut self) {
        self.active_job = None;
        self.overlay = None;
    }

    /// Rebuild the overlay from the current active job state,
    /// preserving scroll position. No-op when no job is active.
    pub(crate) fn rebuild_overlay(&mut self) {
        if let Some(job) = &self.active_job {
            let scroll = self.overlay.as_ref().map(|o| o.scroll.get()).unwrap_or(0);
            self.overlay = Some(job.build_overlay(&self.theme));
            if let Some(ov) = &self.overlay {
                ov.scroll.set(scroll);
            }
        }
    }

    /// Open a py-spy loading overlay and spawn the one-shot HTTP fetch.
    ///
    /// Calls `set_job` which drops any prior variant and its receiver,
    /// cancelling any in-flight fetch (PY-1/PY-2).
    pub(crate) fn start_pyspy(&mut self, proc_id: hyperactor::reference::ProcId) {
        let proc_ref = proc_id.to_string();
        let short = proc_id
            .label()
            .map(|l| l.as_str().to_string())
            .unwrap_or_else(|| proc_id.id().to_string());
        let scheme = self.theme.scheme; // ColorScheme: Copy
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        let timeout = self
            .policy
            .request_timeout(crate::timeouts::RequestOp::PySpyDump);
        let (tx, rx) = oneshot::channel();
        self.set_job(ActiveJob::PySpy {
            rx: Some(rx),
            short,
            lines: vec![],
            completed_at: None,
        });
        // TP-9: timeout covers the full request lifecycle (send +
        // body + parse) at the operation boundary.
        tokio::spawn(async move {
            let url = format!("{}/v1/pyspy/{}", base_url, urlencoding::encode(&proc_ref));
            let result = tokio::time::timeout(timeout, async {
                let resp = client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| format!("request failed: {e}"))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let lines = match resp.json::<serde_json::Value>().await {
                        Ok(json) => parse_error_envelope(&json),
                        Err(_) => vec![Line::from(format!("HTTP {status}"))],
                    };
                    return Ok::<_, String>(lines);
                }
                match resp.json::<serde_json::Value>().await {
                    Err(e) => Ok(vec![Line::from(format!("parse error: {e}"))]),
                    Ok(json) => Ok(pyspy_json_to_lines(&json, &scheme)),
                }
            })
            .await;
            let lines: Vec<Line<'static>> = match result {
                Err(_) => vec![Line::from(format!(
                    "request timed out after {}s",
                    timeout.as_secs()
                ))],
                Ok(Ok(l)) => l,
                Ok(Err(e)) => vec![Line::from(e)],
            };
            let _ = tx.send(lines);
        });
    }

    /// Open a config loading overlay and spawn the one-shot HTTP fetch.
    ///
    /// Calls `set_job` which drops any prior variant and its receiver,
    /// cancelling any in-flight fetch (CFG-1/CFG-2).
    pub(crate) fn start_config(&mut self, proc_id: hyperactor::reference::ProcId) {
        let proc_ref = proc_id.to_string();
        let short = proc_id
            .label()
            .map(|l| l.as_str().to_string())
            .unwrap_or_else(|| proc_id.id().to_string());
        let scheme = self.theme.scheme; // ColorScheme: Copy
        let client = self.client.clone();
        let base_url = self.base_url.clone();
        let timeout = self
            .policy
            .request_timeout(crate::timeouts::RequestOp::ConfigDump);
        let (tx, rx) = oneshot::channel();
        self.set_job(ActiveJob::Config {
            rx: Some(rx),
            short,
            lines: vec![],
            completed_at: None,
        });
        // TP-9: timeout covers the full request lifecycle (send +
        // body + parse) at the operation boundary.
        tokio::spawn(async move {
            let url = format!("{}/v1/config/{}", base_url, urlencoding::encode(&proc_ref));
            let result = tokio::time::timeout(timeout, async {
                let resp = client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| format!("request failed: {e}"))?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let lines = match resp.json::<serde_json::Value>().await {
                        Ok(json) => parse_error_envelope(&json),
                        Err(_) => vec![Line::from(format!("HTTP {status}"))],
                    };
                    return Ok::<_, String>(lines);
                }
                match resp.json::<serde_json::Value>().await {
                    Err(e) => Ok(vec![Line::from(format!("parse error: {e}"))]),
                    Ok(json) => Ok(config_json_to_lines(&json, &scheme)),
                }
            })
            .await;
            let lines: Vec<Line<'static>> = match result {
                Err(_) => vec![Line::from(format!(
                    "request timed out after {}s",
                    timeout.as_secs()
                ))],
                Ok(Ok(l)) => l,
                Ok(Err(e)) => vec![Line::from(e)],
            };
            let _ = tx.send(lines);
        });
    }

    /// Handle a single keypress and update in-memory UI state.
    ///
    /// Returns a `KeyResult` describing whether only the
    /// selection/expand state changed (so the detail pane should be
    /// refreshed) or whether a full topology refresh is required
    /// (e.g. after expanding nodes or toggling system-proc
    /// visibility).
    pub(crate) fn on_key(&mut self, key: KeyEvent) -> KeyResult {
        // When an overlay is active, intercept navigation keys.
        // Shared across all overlay variants; variant-specific rerun
        // keys are dispatched by overlay_rerun_key.
        if self.active_job.is_some() {
            match key.code {
                KeyCode::Esc => {
                    self.dismiss_job();
                }
                KeyCode::Char('q') => {
                    self.should_quit = true;
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.should_quit = true;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if let Some(ov) = &self.overlay {
                        ov.scroll_up();
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if let Some(ov) = &self.overlay {
                        ov.scroll_down();
                    }
                }
                _ => {
                    // Variant-specific rerun keys.
                    return self.overlay_rerun_key(key);
                }
            }
            return KeyResult::None;
        }

        let rows = self.visible_rows();

        match key.code {
            KeyCode::Char('q') => {
                // Quit immediately
                self.should_quit = true;
                KeyResult::None
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Ctrl+C: immediate quit
                self.should_quit = true;
                KeyResult::None
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.cursor.move_up() {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.cursor.move_down() {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Home | KeyCode::Char('g') => {
                if self.cursor.home() {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::End | KeyCode::Char('G') => {
                if self.cursor.end() {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::PageDown => {
                if self.cursor.page_down(10) {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::PageUp => {
                if self.cursor.page_up(10) {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Tab => {
                // Toggle expand/collapse on the selected node.
                if let Some(row) = rows.get(&self.cursor)
                    && row.node.has_children
                {
                    if row.node.expanded {
                        // Collapse.
                        let reference = row.node.reference.clone();
                        let depth = row.depth;
                        if let Some(root) = &mut self.tree
                            && let Some(node) = find_at_depth_from_root_mut(root, &reference, depth)
                        {
                            node.expanded = false;
                            let rows = self.visible_rows();
                            self.cursor.update_len(rows.len());
                            return KeyResult::DetailChanged;
                        }
                    } else {
                        // Expand; lazily fetch children.
                        let reference = row.node.reference.clone();
                        let depth = row.depth;
                        return KeyResult::ExpandNode(reference, depth);
                    }
                }
                KeyResult::None
            }
            KeyCode::Char('c') => {
                // Collapse all top-level nodes.
                self.mutate_tree(|root| {
                    collapse_all(root);
                });
                let rows = self.visible_rows();
                self.cursor.update_len(rows.len());
                KeyResult::DetailChanged
            }
            KeyCode::Char('s') => {
                // Toggle system proc visibility
                self.show_system = !self.show_system;
                KeyResult::NeedsRefresh
            }
            KeyCode::Char('h') => {
                // Toggle stopped actor visibility (failed always visible)
                self.show_stopped = !self.show_stopped;
                KeyResult::NeedsRefresh
            }
            KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Scroll selected item to top of visible area
                self.tree_scroll_offset = self.cursor.pos();
                KeyResult::None
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Page down (Ctrl+D, vi-style)
                if self.cursor.page_down(10) {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Char('d') => {
                // Open diagnostics pane (Esc to close, j/k to scroll).
                KeyResult::RunDiagnostics
            }
            KeyCode::Char('p') => {
                if let Some(proc_ref) = self.pyspy_proc_ref() {
                    KeyResult::RunPySpy(proc_ref)
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Char('C') => {
                // CFG-4: same target resolution as pyspy_proc_ref.
                if let Some(proc_ref) = self.pyspy_proc_ref() {
                    KeyResult::RunConfig(proc_ref)
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Page up (Ctrl+U, vi-style)
                if self.cursor.page_up(10) {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Char('v') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Page down (Ctrl+V, Emacs-style)
                if self.cursor.page_down(10) {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            KeyCode::Char('v') if key.modifiers.contains(KeyModifiers::ALT) => {
                // Page up (Alt+V, Emacs-style)
                if self.cursor.page_up(10) {
                    self.ensure_cursor_visible();
                    KeyResult::DetailChanged
                } else {
                    KeyResult::None
                }
            }
            _ => KeyResult::None,
        }
    }

    /// Dispatch variant-specific rerun keys when an overlay is active.
    pub(crate) fn overlay_rerun_key(&self, key: KeyEvent) -> KeyResult {
        match &self.active_job {
            Some(ActiveJob::Diagnostics { .. }) => match key.code {
                KeyCode::Char('r') | KeyCode::Char('d') => KeyResult::RunDiagnostics,
                _ => KeyResult::None,
            },
            Some(ActiveJob::PySpy { .. }) => match key.code {
                KeyCode::Char('p') => {
                    if let Some(proc_ref) = self.pyspy_proc_ref() {
                        KeyResult::RunPySpy(proc_ref)
                    } else {
                        KeyResult::None
                    }
                }
                _ => KeyResult::None,
            },
            Some(ActiveJob::Config { .. }) => match key.code {
                KeyCode::Char('C') => {
                    if let Some(proc_ref) = self.pyspy_proc_ref() {
                        KeyResult::RunConfig(proc_ref)
                    } else {
                        KeyResult::None
                    }
                }
                _ => KeyResult::None,
            },
            None => KeyResult::None,
        }
    }
}

/// Parse an `ApiErrorEnvelope` JSON body into a single display line.
///
/// Renders `"code: message"` from `{ "error": { "code": ..., "message": ... } }`.
/// Falls back to `"unknown: "` when either field is absent.
pub(crate) fn parse_error_envelope(json: &serde_json::Value) -> Vec<Line<'static>> {
    let code = json
        .get("error")
        .and_then(|e| e.get("code"))
        .and_then(|c| c.as_str())
        .unwrap_or("unknown");
    let msg = json
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("");
    vec![Line::from(format!("{code}: {msg}"))]
}

/// Format a py-spy JSON response into styled display lines.
///
/// ## Ok variant
/// Renders a two-field metadata header (`pid` / `binary` basename),
/// a blank separator, then per-thread sections from the structured
/// `stack_traces` array. Thread headers are styled using
/// `scheme.node_proc` and frame lines using `scheme.node_actor`.
pub(crate) fn pyspy_json_to_lines(
    json: &serde_json::Value,
    scheme: &ColorScheme,
) -> Vec<Line<'static>> {
    if let Some(ok) = json.get("Ok") {
        let pid = ok.get("pid").and_then(|v| v.as_u64());
        let binary = ok
            .get("binary")
            .and_then(|v| v.as_str())
            .unwrap_or("py-spy");
        // Show only the basename to avoid dominating the overlay with a long
        // absolute path (the full path is accessible via the node detail pane).
        let binary_name = std::path::Path::new(binary)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(binary)
            .to_owned();

        let mut lines: Vec<Line<'static>> = vec![];

        // Metadata header: "pid: N  binary: name"
        let mut header: Vec<Span<'static>> = vec![];
        if let Some(p) = pid {
            header.push(Span::styled("pid: ", scheme.detail_label));
            header.push(Span::raw(p.to_string()));
            header.push(Span::raw("  "));
        }
        header.push(Span::styled("binary: ", scheme.detail_label));
        header.push(Span::raw(binary_name));
        lines.push(Line::from(header));
        lines.push(Line::from("")); // blank separator

        let traces = ok
            .get("stack_traces")
            .and_then(|v| v.as_array())
            .map(Vec::as_slice)
            .unwrap_or_default();

        if traces.is_empty() {
            lines.push(Line::from("(empty stack)"));
            return lines;
        }

        for trace in traces {
            // Thread header: "Thread 0x1234 (MainThread) [active, gil]"
            let thread_id = trace.get("thread_id").and_then(|v| v.as_u64()).unwrap_or(0);
            let thread_name = trace
                .get("thread_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let active = trace
                .get("active")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let owns_gil = trace
                .get("owns_gil")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let mut flags = Vec::new();
            if active {
                flags.push("active");
            }
            if owns_gil {
                flags.push("gil");
            }
            let flags_str = if flags.is_empty() {
                String::new()
            } else {
                format!(" [{}]", flags.join(", "))
            };
            let name_part = if thread_name.is_empty() {
                String::new()
            } else {
                format!(" ({})", thread_name)
            };
            lines.push(Line::from(Span::styled(
                format!("Thread 0x{thread_id:x}{name_part}{flags_str}"),
                scheme.node_proc,
            )));

            // Frames, innermost first.
            let frames = trace
                .get("frames")
                .and_then(|v| v.as_array())
                .map(Vec::as_slice)
                .unwrap_or_default();
            for (i, frame) in frames.iter().enumerate() {
                let name = frame.get("name").and_then(|v| v.as_str()).unwrap_or("?");
                let filename = frame
                    .get("short_filename")
                    .and_then(|v| v.as_str())
                    .or_else(|| frame.get("filename").and_then(|v| v.as_str()))
                    .unwrap_or("");
                let line_no = frame.get("line").and_then(|v| v.as_i64()).unwrap_or(0);
                lines.push(Line::from(Span::styled(
                    format!("  #{i:<3} {name} ({filename}:{line_no})"),
                    scheme.node_actor,
                )));
            }
            lines.push(Line::from("")); // blank separator between threads
        }

        // Render non-fatal warnings (e.g., --native-all fallback).
        let warnings = ok
            .get("warnings")
            .and_then(|v| v.as_array())
            .map(Vec::as_slice)
            .unwrap_or_default();
        for w in warnings {
            if let Some(s) = w.as_str() {
                lines.push(Line::from(Span::styled(
                    format!("warn: {s}"),
                    scheme.detail_status_warn,
                )));
            }
        }

        return lines;
    }
    if let Some(nf) = json.get("BinaryNotFound") {
        let mut lines = vec![Line::from(Span::styled(
            "py-spy binary not found",
            scheme.error,
        ))];
        if let Some(arr) = nf.get("searched").and_then(|s| s.as_array()) {
            for p in arr {
                if let Some(s) = p.as_str() {
                    lines.push(Line::from(vec![
                        Span::styled("  searched: ", scheme.detail_label),
                        Span::raw(s.to_owned()),
                    ]));
                }
            }
        }
        return lines;
    }
    if let Some(failed) = json.get("Failed") {
        let mut lines = vec![];
        if let Some(pid) = failed.get("pid").and_then(|v| v.as_u64()) {
            lines.push(Line::from(vec![
                Span::styled("pid: ", scheme.detail_label),
                Span::raw(pid.to_string()),
            ]));
        }
        if let Some(binary) = failed.get("binary").and_then(|v| v.as_str()) {
            lines.push(Line::from(vec![
                Span::styled("binary: ", scheme.detail_label),
                Span::raw(binary.to_owned()),
            ]));
        }
        match failed.get("exit_code") {
            Some(v) if v.is_null() => lines.push(Line::from(vec![
                Span::styled("exit_code: ", scheme.detail_label),
                Span::styled("(killed/timeout)", scheme.detail_status_warn),
            ])),
            Some(v) => lines.push(Line::from(vec![
                Span::styled("exit_code: ", scheme.detail_label),
                Span::styled(v.to_string(), scheme.error),
            ])),
            None => {}
        }
        let stderr = failed.get("stderr").and_then(|s| s.as_str()).unwrap_or("");
        for l in stderr.lines() {
            lines.push(Line::from(l.to_owned()));
        }
        if lines.is_empty() {
            lines.push(Line::from("(py-spy failed, no output)"));
        }
        return lines;
    }
    vec![Line::from(format!("unexpected response: {json}"))]
}

/// Format a `ConfigDumpResult` JSON response into styled `Line`s.
///
/// Groups entries by module prefix, highlights entries where
/// `changed_from_default` is true with the `info` style, and dims
/// default-valued entries.
pub(crate) fn config_json_to_lines(
    json: &serde_json::Value,
    scheme: &ColorScheme,
) -> Vec<Line<'static>> {
    let entries = match json.get("entries").and_then(|e| e.as_array()) {
        Some(arr) => arr,
        None => return vec![Line::from(format!("unexpected response: {json}"))],
    };

    if entries.is_empty() {
        return vec![Line::from("(no config entries)")];
    }

    let mut lines = Vec::new();
    let mut current_module: Option<String> = None;

    for entry in entries {
        let name = entry.get("name").and_then(|n| n.as_str()).unwrap_or("");
        let value = entry.get("value").and_then(|v| v.as_str()).unwrap_or("");
        let source = entry.get("source").and_then(|s| s.as_str()).unwrap_or("?");
        let changed = entry
            .get("changed_from_default")
            .and_then(|c| c.as_bool())
            .unwrap_or(false);

        // Group by module prefix (everything before the last `::`).
        let module = name.rsplit_once("::").map(|(m, _)| m).unwrap_or(name);
        if current_module.as_deref() != Some(module) {
            if current_module.is_some() {
                lines.push(Line::from(""));
            }
            lines.push(Line::from(Span::styled(
                format!("  {module}"),
                ratatui::style::Style::default().add_modifier(ratatui::style::Modifier::BOLD),
            )));
            current_module = Some(module.to_string());
        }

        // Key name is the last segment after `::`.
        let key_name = name.rsplit_once("::").map(|(_, k)| k).unwrap_or(name);

        // Key column: always bold label style for readability.
        let key_style = scheme.detail_label;

        // Value column: highlight changed values, dim defaults.
        let value_style = if changed {
            scheme.info
        } else {
            scheme.detail_stopped // dimmed — default value, nothing to notice
        };

        // Source column: color-coded by layer provenance.
        let source_style = match source {
            "Default" => scheme.detail_stopped,     // dimmed — uninteresting
            "Env" => scheme.stat_timing,            // yellow — env override stands out
            "Runtime" => scheme.stat_selection,     // purple — programmatic override
            "TestOverride" => scheme.error,         // red — test-only, should not appear in prod
            "ClientOverride" => scheme.stat_system, // blue — client-sent config
            "File" => scheme.node_actor,            // muted blue — file-based config
            _ => scheme.detail_label,               // fallback
        };

        lines.push(Line::from(vec![
            Span::styled(format!("    {key_name:<40}"), key_style),
            Span::styled(format!("{value:<20}"), value_style),
            Span::styled(source.to_string(), source_style),
        ]));
    }

    lines
}

/// Await the next event from whichever overlay-producing job is currently live.
///
/// Returns `std::future::pending()` when `active_job` is `None` or when
/// neither receiver is ready, so the `tokio::select!` arm is never woken —
/// equivalent to disabling the arm without requiring conditional compilation.
///
/// A single function (rather than two separate `recv_diag`/`recv_pyspy` calls)
/// is necessary so that `tokio::select!` holds only one `&mut active_job` borrow
/// at a time.
async fn recv_active_job(job: &mut Option<ActiveJob>) -> ActiveJobEvent {
    match job {
        Some(ActiveJob::Diagnostics { rx: Some(rx), .. }) => {
            ActiveJobEvent::DiagResult(rx.recv().await)
        }
        Some(ActiveJob::PySpy {
            rx: Some(inner), ..
        }) => {
            let lines = match inner.await {
                Ok(l) => l,
                Err(_) => vec![Line::from("(fetch task dropped)")],
            };
            ActiveJobEvent::PySpyResult(lines)
        }
        Some(ActiveJob::Config {
            rx: Some(inner), ..
        }) => {
            let lines = match inner.await {
                Ok(l) => l,
                Err(_) => vec![Line::from("(fetch task dropped)")],
            };
            ActiveJobEvent::ConfigResult(lines)
        }
        _ => std::future::pending().await,
    }
}

/// TP-10: derive refresh policy from active job state.
///
/// Suspend refresh only while a foreground operation is in flight.
/// Completed overlays (results displayed, waiting for Esc) do not
/// suppress refresh — the user is reading results, not waiting for
/// a network operation. The mapping is local to the app module so
/// `timeouts::RefreshPolicy` stays independent of UI job types.
pub(crate) fn refresh_policy_for_job(job: &Option<ActiveJob>) -> crate::timeouts::RefreshPolicy {
    use crate::timeouts::RefreshPolicy;
    match job {
        None => RefreshPolicy::Baseline,
        Some(ActiveJob::Diagnostics { running: true, .. }) => RefreshPolicy::Suspend,
        Some(ActiveJob::PySpy { rx: Some(_), .. }) => RefreshPolicy::Suspend,
        Some(ActiveJob::Config { rx: Some(_), .. }) => RefreshPolicy::Suspend,
        // Completed overlays: operation finished, user is reading results.
        Some(_) => RefreshPolicy::Baseline,
    }
}

/// Drive the main event loop for the admin TUI.
///
/// Periodically refreshes topology from the admin API, renders the UI
/// each tick, and processes keyboard input until the user exits.
pub(crate) async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    mut app: App,
) -> io::Result<()> {
    let refresh_ms = app.policy.refresh_interval.as_millis() as u64;
    let mut refresh_interval = tokio::time::interval(app.policy.refresh_interval);
    app.refresh_interval_label = if refresh_ms >= 1000 && refresh_ms.is_multiple_of(1000) {
        format!("{}s", refresh_ms / 1000)
    } else {
        format!("{}ms", refresh_ms)
    };
    let mut events = EventStream::new();

    loop {
        // Update viewport height before rendering. The body area is
        // terminal height minus header (3 rows) and footer (2 rows).
        let terminal_size = terminal.size()?;
        app.tree_viewport_height = terminal_size.height.saturating_sub(5) as usize;

        terminal.draw(|frame| ui(frame, &app))?;

        tokio::select! {
            _ = refresh_interval.tick() => {
                // Phase 3a: the effective refresh policy is the
                // join of all refresh-pressure sources. Currently
                // there is one source (foreground job state); later
                // sources extend by more joins, not by rewriting
                // the branch. Refresh runs only when effective
                // policy is Baseline. When the in-flight operation
                // completes, refresh resumes on the next scheduled
                // tick, not immediately.
                use algebra::JoinSemilattice;
                use crate::timeouts::RefreshPolicy;
                let effective = RefreshPolicy::Baseline
                    .join(&refresh_policy_for_job(&app.active_job));
                if effective == RefreshPolicy::Baseline {
                    app.refresh().await;
                }
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
                            KeyResult::ExpandNode(reference, depth) => {
                                if app.expand_node(&reference, depth).await {
                                    // Update cursor length to reflect new children
                                    let rows = app.visible_rows();
                                    app.cursor.update_len(rows.len());
                                    // Move cursor to first child after expanding
                                    app.cursor.move_down();
                                    app.ensure_cursor_visible();
                                }
                                app.update_selected_detail().await;
                            }
                            KeyResult::RunDiagnostics => {
                                let rx = run_diagnostics(
                                    app.client.clone(),
                                    app.base_url.clone(),
                                    &app.policy,
                                );
                                // PY-5: set_job drops any prior PySpy variant.
                                app.set_job(ActiveJob::Diagnostics {
                                    results: Vec::new(),
                                    running: true,
                                    rx: Some(rx),
                                    completed_at: None,
                                });
                            }
                            KeyResult::RunPySpy(proc_id) => {
                                app.start_pyspy(proc_id);
                            }
                            KeyResult::RunConfig(proc_id) => {
                                app.start_config(proc_id);
                            }
                            KeyResult::None => {}
                        }
                    }
                    Some(Ok(Event::Resize(_, _))) => {}
                    _ => {}
                }
            }
            job_event = recv_active_job(&mut app.active_job) => {
                if let Some(job) = &mut app.active_job {
                    job.on_event(job_event);
                }
                app.rebuild_overlay();
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}
