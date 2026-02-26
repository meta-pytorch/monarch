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
use std::time::Duration;

use crossterm::event::Event;
use crossterm::event::EventStream;
use crossterm::event::KeyCode;
use crossterm::event::KeyEvent;
use crossterm::event::KeyModifiers;
use futures::StreamExt;
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use crate::Args;
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
use crate::collect_refs;
use crate::derive_label;
use crate::fetch_with_join;
use crate::find_at_depth_from_root_mut;
use crate::flatten_tree;
use crate::get_cached_payload;
use crate::is_failed_node;
use crate::is_stopped_node;
use crate::is_system_node;
use crate::render::ui;
use crate::sorted_children;

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
    /// Failed nodes are always visible regardless of this setting.
    pub(crate) show_stopped: bool,

    /// Fetch cache with generation-based staleness.
    pub(crate) fetch_cache: HashMap<String, FetchState<NodePayload>>,
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
    ) -> Self {
        Self {
            base_url,
            client,
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
        reference: &str,
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
        )
        .await
    }

    /// Extract a payload from FetchState if Ready, otherwise None.
    pub(crate) fn get_cached_payload(&self, reference: &str) -> Option<&NodePayload> {
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
    pub(crate) fn selected_reference(&self) -> Option<&str> {
        let rows = self.visible_rows();
        rows.get(&self.cursor)
            .map(|row| row.node.reference.as_str())
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

        // Save expanded state before rebuilding.
        // Track (reference, depth) pairs to handle dual appearances correctly.
        let mut expanded_keys = HashSet::new();
        if let Some(root) = self.tree() {
            // Start at depth -1 so root's children are at depth 0
            for child in &root.children {
                collect_expanded_refs(child, 0, &mut expanded_keys);
            }
        }

        // Save current selection's reference and depth.
        let rows = self.visible_rows();
        let prev_selection = rows
            .get(&self.cursor)
            .map(|row| (row.node.reference.clone(), row.depth));

        // Fetch root using centralized fetch with force=true.
        let root_state = self.fetch_node_state("root", true).await;
        let root_payload = match root_state {
            FetchState::Ready { value, .. } => value,
            FetchState::Error { msg, .. } => {
                self.error = Some(format!("Failed to connect: {}", msg));
                return;
            }
            FetchState::Unknown => return,
        };

        // Path for cycle detection: tracks current path from root to
        // Node being built. Start with "root" in the path.
        let mut path = vec!["root".to_string()];

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
                self.refresh_gen,
                &mut self.seq_counter,
            )
            .await
            {
                root_children.push(child_node);
            }
        }

        // Create synthetic root node.
        self.set_tree(Some(TreeNode {
            reference: "root".to_string(),
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
        let live_refs: HashSet<String> = if let Some(root) = self.tree() {
            let mut refs = HashSet::new();
            collect_refs(root, &mut refs);
            refs.into_iter().map(|s| s.to_string()).collect()
        } else {
            HashSet::new()
        };
        self.fetch_cache
            .retain(|k, _| k == "root" || live_refs.contains(k.as_str()));

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
    pub(crate) async fn expand_node(&mut self, reference: &str, depth: usize) -> bool {
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
        let system_children: HashSet<&str> = match &payload.properties {
            NodeProperties::Root {
                system_children, ..
            }
            | NodeProperties::Host {
                system_children, ..
            }
            | NodeProperties::Proc {
                system_children, ..
            } => system_children.iter().map(|s| s.as_str()).collect(),
            _ => HashSet::new(),
        };

        // Extract stopped_children from proc payloads for lazy
        // filtering/graying without per-child fetches.
        let (stopped_children, parent_is_poisoned): (HashSet<&str>, bool) =
            match &payload.properties {
                NodeProperties::Proc {
                    stopped_children,
                    is_poisoned,
                    ..
                } => (
                    stopped_children.iter().map(|s| s.as_str()).collect(),
                    *is_poisoned,
                ),
                _ => (HashSet::new(), false),
            };

        let mut child_nodes = Vec::new();
        for child_ref in &children {
            // Filter order: system first, then stopped.
            if !self.show_system && system_children.contains(child_ref.as_str()) {
                continue;
            }

            let child_is_stopped = stopped_children.contains(child_ref.as_str());
            let child_is_system = system_children.contains(child_ref.as_str());

            // Failed nodes are always visible (never filtered by show_stopped).
            // If the parent proc is poisoned, its stopped children may be
            // failed — don't filter them out (cache may be empty on first load).
            let child_is_failed = parent_is_poisoned
                || self
                    .get_cached_payload(child_ref.as_str())
                    .is_some_and(|c| is_failed_node(&c.properties));
            if !self.show_stopped && child_is_stopped && !child_is_failed {
                continue;
            }

            if is_proc_or_actor {
                // Lazy: use placeholder or cached payload.
                if let Some(cached) = self.get_cached_payload(child_ref.as_str()) {
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
                    // Apply stopped filtering (failed nodes always visible).
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
        let reference = self.selected_reference().map(|s| s.to_string());

        if let Some(ref_str) = reference {
            let state = self.fetch_node_state(&ref_str, false).await;
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

    /// Handle a single keypress and update in-memory UI state.
    ///
    /// Returns a `KeyResult` describing whether only the
    /// selection/expand state changed (so the detail pane should be
    /// refreshed) or whether a full topology refresh is required
    /// (e.g. after expanding nodes or toggling system-proc
    /// visibility).
    pub(crate) fn on_key(&mut self, key: KeyEvent) -> KeyResult {
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
}

/// Drive the main event loop for the admin TUI.
///
/// Periodically refreshes topology from the admin API, renders the UI
/// each tick, and processes keyboard input until the user exits.
pub(crate) async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    args: &Args,
    mut app: App,
) -> io::Result<()> {
    let mut refresh_interval = tokio::time::interval(Duration::from_millis(args.refresh_ms));
    app.refresh_interval_label = if args.refresh_ms >= 1000 && args.refresh_ms.is_multiple_of(1000)
    {
        format!("{}s", args.refresh_ms / 1000)
    } else {
        format!("{}ms", args.refresh_ms)
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
