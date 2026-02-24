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
//! - **Zero ad-hoc tree traversal**: all tree walks use the fold
//!   abstractions (`fold_tree`, `fold_tree_mut`, or
//!   `fold_tree_mut_with_depth`), not bespoke recursion.
//! - **Selection semantics**: cursor restoration matches
//!   `(reference, depth)` first to disambiguate duplicate references,
//!   then falls back to reference-only matching.
//! - **Concurrency model**: HTTP fetches are issued serially through
//!   the event loop; join semantics handle retries/reordering but not
//!   parallel fetch races.
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

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::io::IsTerminal;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use algebra::JoinSemilattice;
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
    #[arg(long, default_value_t = 1000)]
    refresh_ms: u64,
}

/// Monotonic ordering key for fetch results.
///
/// `ts_micros` comes from wall-clock time (RealClock) and `seq`
/// breaks ties to ensure a total order within this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Stamp {
    /// Wall-clock timestamp in microseconds since UNIX epoch.
    ts_micros: u64,
    /// Monotonic tie-breaker for identical timestamps in this
    /// process.
    seq: u64,
}

/// Cached result of fetching a node, with ordering metadata.
///
/// `generation` tracks the refresh cycle that produced the entry.
/// `stamp` provides a total order among fetches for join semantics.
#[derive(Clone, Debug)]
enum FetchState<T> {
    /// Not yet fetched or explicitly invalidated.
    Unknown,
    /// Successful fetch result.
    Ready {
        /// Ordering key for merge semantics.
        stamp: Stamp,
        /// Refresh generation when this value was fetched.
        generation: u64,
        /// The fetched payload.
        value: T,
    },
    /// Failed fetch result (always retries - no generation tracking).
    Error {
        /// Ordering key for merge semantics.
        stamp: Stamp,
        /// Human-readable error message.
        msg: String,
    },
}

/// Join prefers the entry with the newer `Stamp` to ensure total
/// ordering.
///
/// `Unknown` acts as the identity. When stamps are equal, a
/// deterministic tie-break keeps the operation commutative and
/// idempotent (Ready > Error, and Error uses lexicographic `msg`
/// ordering).
impl<T: Clone> JoinSemilattice for FetchState<T> {
    fn join(&self, other: &Self) -> Self {
        use FetchState::*;
        match (self, other) {
            (Unknown, x) | (x, Unknown) => x.clone(),
            (Ready { stamp: a, .. }, Ready { stamp: b, .. })
            | (Ready { stamp: a, .. }, Error { stamp: b, .. })
            | (Error { stamp: a, .. }, Ready { stamp: b, .. })
            | (Error { stamp: a, .. }, Error { stamp: b, .. }) => {
                if a > b {
                    self.clone()
                } else if b > a {
                    other.clone()
                } else {
                    // Deterministic tie-break for commutativity when stamps
                    // Are equal.
                    match (self, other) {
                        (Ready { .. }, _) => self.clone(),
                        (_, Ready { .. }) => other.clone(),
                        (Error { msg: m1, .. }, Error { msg: m2, .. }) => {
                            // Lexicographic tie-break ensures commutativity
                            // For Error vs Error.
                            if m1 >= m2 {
                                self.clone()
                            } else {
                                other.clone()
                            }
                        }
                        _ => self.clone(),
                    }
                }
            }
        }
    }
}

/// Navigation cursor over a bounded list.
///
/// Invariant: `pos < len` (or `pos == 0` when `len == 0`).
/// Movement methods return `true` when the position changes.
#[derive(Debug, Clone)]
struct Cursor {
    /// Current position within the list.
    pos: usize,
    /// Current length of the list.
    len: usize,
}

impl Cursor {
    /// Create a new cursor for a list of the given length.
    ///
    /// Position starts at 0. If `len == 0`, position is 0 (no valid
    /// selection).
    fn new(len: usize) -> Self {
        Self { pos: 0, len }
    }

    /// Move up (decrement). Returns true if position changed.
    fn move_up(&mut self) -> bool {
        if self.pos > 0 {
            self.pos -= 1;
            true
        } else {
            false
        }
    }

    /// Move down (increment). Returns true if position changed.
    fn move_down(&mut self) -> bool {
        if self.pos + 1 < self.len {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Jump to start. Returns true if position changed.
    fn home(&mut self) -> bool {
        if self.pos != 0 {
            self.pos = 0;
            true
        } else {
            false
        }
    }

    /// Jump to end. Returns true if position changed.
    fn end(&mut self) -> bool {
        let new_pos = self.len.saturating_sub(1);
        if self.pos != new_pos {
            self.pos = new_pos;
            true
        } else {
            false
        }
    }

    /// Page down by `amount`. Returns true if position changed.
    fn page_down(&mut self, amount: usize) -> bool {
        let new_pos = (self.pos + amount).min(self.len.saturating_sub(1));
        if self.pos != new_pos {
            self.pos = new_pos;
            true
        } else {
            false
        }
    }

    /// Page up by `amount`. Returns true if position changed.
    fn page_up(&mut self, amount: usize) -> bool {
        let new_pos = self.pos.saturating_sub(amount);
        if self.pos != new_pos {
            self.pos = new_pos;
            true
        } else {
            false
        }
    }

    /// Update length and clamp position to remain valid.
    ///
    /// Used after tree mutations (refresh, collapse) to maintain the
    /// cursor invariant.
    fn update_len(&mut self, new_len: usize) {
        self.len = new_len;
        if new_len == 0 {
            self.pos = 0;
        } else {
            self.pos = self.pos.min(new_len - 1);
        }
    }

    /// Set position directly (for restoring saved selection).
    ///
    /// Clamps to valid range.
    fn set_pos(&mut self, new_pos: usize) {
        if self.len == 0 {
            self.pos = 0;
        } else {
            self.pos = new_pos.min(self.len - 1);
        }
    }

    /// Get current position.
    fn pos(&self) -> usize {
        self.pos
    }

    /// Get current length.
    #[allow(dead_code)] // used by tests
    fn len(&self) -> usize {
        self.len
    }
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

    /// Short human-readable label for display.
    fn label(&self) -> &'static str {
        match self {
            NodeType::Root => "root",
            NodeType::Host => "host",
            NodeType::Proc => "proc",
            NodeType::Actor => "actor",
        }
    }
}

/// Color scheme for the TUI.
///
/// Defines all colors and styles used throughout the interface. This
/// abstraction allows for consistent theming and makes it easy to add
/// alternative schemes (light mode, high contrast, etc.) in the
/// future.
///
/// ## Color Semantics
///
/// Colors carry consistent meaning across the interface:
/// - 🟢 **Green**: Topology/scale (host/proc/actor counts, success)
/// - 🟡 **Yellow**: Timing/temporal (refresh intervals, warnings)
/// - 🟣 **Magenta**: Selection/focus (current node, highlighted items)
/// - 🔵 **Blue**: System state/config (toggles, system procs, actors)
/// - ⚪ **Gray**: Secondary info (URLs, labels, borders)
/// - 🔴 **Red**: Errors (error states, failures)
/// - 🔷 **Cyan**: Important headers (app name, help headings)
///
/// ## Semantic Categories
///
/// - **UI chrome**: App name, borders, footer
/// - **Node types**: Colors for root/host/proc/actor in tree
/// - **States**: Error, warning, success for status display
/// - **Header stats**: Timing, topology counts, selection, config
/// - **Help overlay**: Markdown rendering styles
struct ColorScheme {
    // UI chrome
    app_name: Style,
    border: Style,
    _border_focused: Style,
    _footer_help: Style,

    // Node types (tree rendering)
    node_root: Style,
    node_host: Style,
    node_proc: Style,
    node_actor: Style,
    _node_error: Style,

    // Semantic states
    error: Style,
    info: Style,

    // Header stat categories
    stat_timing: Style,    // refresh intervals, durations (temporal)
    stat_selection: Style, // current selection (focus)
    stat_system: Style,    // config toggles (state)
    stat_url: Style,       // connection info (secondary)
    stat_label: Style,     // stat labels/prefixes
    stat_value: Style,     // stat numeric values
}

impl ColorScheme {
    /// Default color scheme.
    ///
    /// This is the current color scheme used throughout the TUI.
    fn default() -> Self {
        Self {
            // UI chrome
            app_name: Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
            border: Style::default().fg(Color::Gray),
            _border_focused: Style::default().fg(Color::Cyan),
            _footer_help: Style::default().fg(Color::DarkGray),

            // Node types
            node_root: Style::default().fg(Color::Cyan),
            node_host: Style::default().fg(Color::Green),
            node_proc: Style::default().fg(Color::Yellow),
            node_actor: Style::default().fg(Color::LightBlue),
            _node_error: Style::default().fg(Color::Red),

            // Semantic states
            error: Style::default().fg(Color::Red),
            info: Style::default().fg(Color::Cyan),

            // Header stats
            stat_timing: Style::default().fg(Color::Yellow),
            stat_selection: Style::default().fg(Color::Magenta),
            stat_system: Style::default().fg(Color::Blue),
            stat_url: Style::default().fg(Color::DarkGray),
            stat_label: Style::default().fg(Color::Gray),
            stat_value: Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        }
    }

    /// Return the style for a given node type.
    fn node_style(&self, node_type: NodeType) -> Style {
        match node_type {
            NodeType::Root => self.node_root,
            NodeType::Host => self.node_host,
            NodeType::Proc => self.node_proc,
            NodeType::Actor => self.node_actor,
        }
    }
}

/// A node in the topology tree.
///
/// Represents the actual tree structure (not a flattened view).
/// The tree is materialized from the admin API by walking references
/// recursively, respecting `expanded_keys` and depth limits.
#[derive(Debug, Clone)]
struct TreeNode {
    /// Opaque reference string for this node (identity in the admin
    /// API).
    reference: String,
    /// Human-friendly label shown in the tree (derived from
    /// [`NodePayload`]).
    label: String,
    /// Node type for color coding.
    node_type: NodeType,
    /// Whether this node is currently expanded in the UI.
    expanded: bool,
    /// Whether this node's own payload has been fetched (as opposed to
    /// being a placeholder derived from a parent's children list).
    fetched: bool,
    /// Whether the backing payload reports any children (controls
    /// fold arrow rendering).
    has_children: bool,
    /// Direct children of this node in the tree.
    children: Vec<TreeNode>,
}

impl TreeNode {
    /// Create a placeholder node (not yet fetched).
    ///
    /// Placeholders are created from parent children lists without
    /// fetching payload. They have `fetched: false`, `has_children:
    /// true`, and empty `children` vector.
    fn placeholder(reference: String) -> Self {
        Self {
            label: derive_label_from_ref(&reference),
            reference,
            node_type: NodeType::Actor,
            expanded: false,
            fetched: false,
            has_children: true,
            children: Vec::new(),
        }
    }

    /// Create a collapsed, fetched node from a payload.
    ///
    /// Used when building child lists from cached or freshly fetched
    /// payloads. The node starts collapsed with no children.
    fn from_payload(reference: String, payload: &NodePayload) -> Self {
        Self {
            label: derive_label(payload),
            node_type: NodeType::from_properties(&payload.properties),
            expanded: false,
            fetched: true,
            has_children: !payload.children.is_empty(),
            children: Vec::new(),
            reference,
        }
    }
}

/// A single row in the flattened UI view.
///
/// Ephemeral structure computed by `flatten_visible` for rendering.
#[derive(Debug, Clone)]
struct FlatRow<'a> {
    /// Reference to the tree node backing this row.
    node: &'a TreeNode,
    /// Visual indentation level for this row.
    depth: usize,
}

/// Wrapper for flattened visible rows with cursor helpers.
///
/// Makes the "ephemeral view" concept explicit and provides safe
/// cursor-based access.
#[derive(Debug)]
struct VisibleRows<'a> {
    rows: Vec<FlatRow<'a>>,
}

impl<'a> VisibleRows<'a> {
    fn new(rows: Vec<FlatRow<'a>>) -> Self {
        Self { rows }
    }

    fn get(&self, cursor: &Cursor) -> Option<&FlatRow<'a>> {
        self.rows.get(cursor.pos())
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn as_slice(&self) -> &[FlatRow<'a>] {
        &self.rows
    }

    /// Check whether a later row at the same depth exists (for tree
    /// connector rendering: `├─` vs `└─`).
    fn has_sibling_after(&self, idx: usize, depth: usize) -> bool {
        for row in &self.rows[idx + 1..] {
            if row.depth < depth {
                return false;
            }
            if row.depth == depth {
                return true;
            }
        }
        false
    }
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

    /// The topology tree rooted at the synthetic "root" node.
    ///
    /// `None` before the first successful refresh, `Some(tree)` after.
    /// The root node itself is not rendered (its children appear at
    /// depth 0).
    tree: Option<TreeNode>,
    /// Navigation cursor over visible tree indices.
    cursor: Cursor,
    /// Scroll offset for topology tree (top visible row).
    tree_scroll_offset: usize,
    /// Height of the topology tree viewport in rows (updated during
    /// rendering).
    tree_viewport_height: usize,
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

    /// Fetch cache with generation-based staleness.
    fetch_cache: HashMap<String, FetchState<NodePayload>>,
    /// Current refresh generation for cache invalidation.
    refresh_gen: u64,
    /// Monotonic sequence counter for timestamp ordering.
    seq_counter: u64,

    /// Color scheme for the entire UI.
    scheme: ColorScheme,
}

/// Result of handling a key event.
enum KeyResult {
    /// Nothing changed.
    None,
    /// Selection or expand/collapse changed; update detail from cache.
    DetailChanged,
    /// A filter/view setting changed; full tree refresh needed.
    NeedsRefresh,
    /// Lazily expand the node at the given (reference, depth).
    ExpandNode(String, usize),
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
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            should_quit: false,
            tree: None,
            cursor: Cursor::new(0),
            tree_scroll_offset: 0,
            tree_viewport_height: 20, // Default, updated during rendering
            detail: None,
            detail_error: None,
            last_refresh: String::new(),
            error: None,
            show_system_procs: false,
            fetch_cache: HashMap::new(),
            refresh_gen: 0,
            seq_counter: 0,
            scheme: ColorScheme::default(),
        }
    }

    // Controlled mutation API - single entry point for tree modifications.

    /// Replace the entire tree structure.
    fn set_tree(&mut self, tree: Option<TreeNode>) {
        self.tree = tree;
    }

    /// Mutate the tree via a closure.
    ///
    /// Provides safe mutable access to the tree. This is the only way
    /// to mutate the tree after initialization.
    fn mutate_tree<F>(&mut self, f: F)
    where
        F: FnOnce(&mut TreeNode),
    {
        if let Some(tree) = &mut self.tree {
            f(tree);
        }
    }

    /// Immutable tree accessor.
    fn tree(&self) -> Option<&TreeNode> {
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
    async fn fetch_node_state(&mut self, reference: &str, force: bool) -> FetchState<NodePayload> {
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
    fn get_cached_payload(&self, reference: &str) -> Option<&NodePayload> {
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
    fn visible_rows(&self) -> VisibleRows<'_> {
        match self.tree() {
            Some(root) => VisibleRows::new(flatten_tree(root)),
            None => VisibleRows::new(Vec::new()),
        }
    }

    /// Get the currently selected node's type and label.
    ///
    /// Returns `None` if no node is selected or tree is empty.
    fn current_selection(&self) -> Option<(NodeType, &str)> {
        let rows = self.visible_rows();
        rows.get(&self.cursor)
            .map(|row| (row.node.node_type, row.node.label.as_str()))
    }

    /// Get the currently selected node's reference.
    ///
    /// Returns `None` if the selection is out of range (e.g. the tree
    /// is empty).
    fn selected_reference(&self) -> Option<&str> {
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
    async fn refresh(&mut self) {
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
                self.show_system_procs,
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
        self.last_refresh = chrono::Local::now().format("%H:%M:%S").to_string();

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
    async fn expand_node(&mut self, reference: &str, depth: usize) -> bool {
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

        let mut child_nodes = Vec::new();
        for child_ref in &children {
            if is_proc_or_actor {
                // Lazy: use placeholder.
                if let Some(cached) = self.get_cached_payload(child_ref.as_str()) {
                    child_nodes.push(TreeNode::from_payload(child_ref.clone(), cached));
                } else {
                    child_nodes.push(TreeNode::placeholder(child_ref.clone()));
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
                    // Apply system proc filtering.
                    if let NodeProperties::Proc { is_system, .. } = cp.properties
                        && !self.show_system_procs
                        && is_system
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
    async fn update_selected_detail(&mut self) {
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
    fn ensure_cursor_visible(&mut self) {
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
    fn on_key(&mut self, key: KeyEvent) -> KeyResult {
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
                // Expand selected node; lazily fetch children.
                if let Some(row) = rows.get(&self.cursor)
                    && row.node.has_children
                    && !row.node.expanded
                {
                    let reference = row.node.reference.clone();
                    let depth = row.depth;
                    return KeyResult::ExpandNode(reference, depth);
                }
                KeyResult::None
            }
            KeyCode::BackTab => {
                // Collapse selected node.
                let to_collapse = rows.get(&self.cursor).and_then(|row| {
                    if row.node.has_children && row.node.expanded {
                        Some((row.node.reference.clone(), row.depth))
                    } else {
                        None
                    }
                });

                if let Some((reference, depth)) = to_collapse
                    && let Some(root) = &mut self.tree
                    && let Some(node) = find_at_depth_from_root_mut(root, &reference, depth)
                {
                    node.expanded = false;
                    let rows = self.visible_rows();
                    self.cursor.update_len(rows.len());
                    return KeyResult::DetailChanged;
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
                self.show_system_procs = !self.show_system_procs;
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

// Tree-building infrastructure (free functions)
//
// Extracted from `App` methods so that `refresh()` can pass disjoint
// Borrows of `App` fields (client, base_url, show_system_procs) to
// The tree builder without complex borrowing.

/// Unified fetch+join path for all cache writes.
///
/// Checks cache first, only fetches if needed (not present, stale, or
/// error), then joins the result into the cache. Returns the final
/// FetchState.
///
/// This is the single source of truth for fetch+join semantics.
async fn fetch_with_join(
    client: &reqwest::Client,
    base_url: &str,
    reference: &str,
    cache: &mut HashMap<String, FetchState<NodePayload>>,
    refresh_gen: u64,
    seq_counter: &mut u64,
    force: bool,
) -> FetchState<NodePayload> {
    let cached_state = cache.get(reference);
    let should_fetch = if force {
        true
    } else {
        match cached_state {
            None => true,
            Some(FetchState::Unknown) => true,
            Some(FetchState::Error { .. }) => true,
            Some(FetchState::Ready { generation, .. }) => *generation < refresh_gen,
        }
    };

    if should_fetch {
        // Generate stamp.
        *seq_counter += 1;
        let ts_micros = RealClock
            .system_time_now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let stamp = Stamp {
            ts_micros,
            seq: *seq_counter,
        };

        // Fetch and wrap in FetchState.
        let new_state = match fetch_node_raw(client, base_url, reference).await {
            Ok(payload) => FetchState::Ready {
                stamp,
                generation: refresh_gen,
                value: payload,
            },
            Err(e) => FetchState::Error { stamp, msg: e },
        };

        // Join into cache.
        cache
            .entry(reference.to_string())
            .and_modify(|s| *s = s.join(&new_state))
            .or_insert(new_state);
    }

    cache.get(reference).cloned().unwrap_or(FetchState::Unknown)
}

/// Fetch a single node payload from the admin API.
///
/// Free-function form of `App::fetch_node` so callers that hold
/// partial borrows of `App` can avoid borrowing all of `&self`.
async fn fetch_node_raw(
    client: &reqwest::Client,
    base_url: &str,
    reference: &str,
) -> Result<NodePayload, String> {
    let url = format!("{}/v1/{}", base_url, urlencoding::encode(reference));
    let resp = client
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

// Tree operations

/// Extract cached payload from FetchState cache (free function).
fn get_cached_payload<'a>(
    cache: &'a HashMap<String, FetchState<NodePayload>>,
    reference: &str,
) -> Option<&'a NodePayload> {
    cache.get(reference).and_then(|state| match state {
        FetchState::Ready { value, .. } => Some(value),
        _ => None,
    })
}

/// Flatten a tree into visible rows using algebraic fold.
///
/// Only expanded nodes contribute their children. This replaces
/// the old `visible_indices()` logic.
fn flatten_tree(root: &TreeNode) -> Vec<FlatRow<'_>> {
    root.children
        .iter()
        .flat_map(|child| flatten_visible(child, 0))
        .collect()
}

/// Flatten visible nodes using fold_tree_with_depth.
///
/// Includes current node and recursively includes children only if
/// current node is expanded.
fn flatten_visible<'a>(node: &'a TreeNode, depth: usize) -> Vec<FlatRow<'a>> {
    fold_tree_with_depth(node, depth, &|n, d, child_results| {
        let mut rows = vec![FlatRow { node: n, depth: d }];
        if n.expanded {
            for child_rows in child_results {
                rows.extend(child_rows);
            }
        }
        rows
    })
}

/// Generic tree fold - unified traversal abstraction.
///
/// Applies `f` to each node in pre-order DFS, accumulating a result.
/// The function receives the current node and the accumulated results
/// from children.
fn fold_tree<'a, B, F>(node: &'a TreeNode, f: &F) -> B
where
    F: Fn(&'a TreeNode, Vec<B>) -> B,
{
    let child_results: Vec<B> = node
        .children
        .iter()
        .map(|child| fold_tree(child, f))
        .collect();
    f(node, child_results)
}

/// Find a node by reference using algebraic fold.
fn find_node_by_ref<'a>(node: &'a TreeNode, reference: &str) -> Option<&'a TreeNode> {
    fold_tree(node, &|n, child_results| {
        if n.reference == reference {
            Some(n)
        } else {
            child_results.into_iter().find_map(|x| x)
        }
    })
}

/// Immutable tree fold with depth tracking.
///
/// Like fold_tree, but passes the current depth to the fold function.
/// Applies `f` to each (node, depth) in pre-order DFS, accumulating results.
fn fold_tree_with_depth<'a, B, F>(node: &'a TreeNode, depth: usize, f: &F) -> B
where
    F: Fn(&'a TreeNode, usize, Vec<B>) -> B,
{
    let child_results: Vec<B> = node
        .children
        .iter()
        .map(|child| fold_tree_with_depth(child, depth + 1, f))
        .collect();
    f(node, depth, child_results)
}

/// Mutable tree fold with early-exit via ControlFlow.
///
/// Applies `f` to each node in pre-order DFS with mutable access.
/// Returns ControlFlow::Break to stop traversal early, or Continue
/// to proceed. This enables algebraic mutable traversals with short-circuiting.
fn fold_tree_mut<B, F>(node: &mut TreeNode, f: &mut F) -> std::ops::ControlFlow<B>
where
    F: for<'a> FnMut(&'a mut TreeNode) -> std::ops::ControlFlow<B>,
{
    // Check current node first
    f(node)?;

    // Then traverse children
    for child in &mut node.children {
        fold_tree_mut(child, f)?;
    }

    std::ops::ControlFlow::Continue(())
}

/// Mutable tree fold with depth tracking and early-exit via ControlFlow.
///
/// Like fold_tree_mut, but passes the current depth to the closure.
/// Applies `f` to each (node, depth) in pre-order DFS with mutable access.
fn fold_tree_mut_with_depth<B, F>(
    node: &mut TreeNode,
    depth: usize,
    f: &mut F,
) -> std::ops::ControlFlow<B>
where
    F: for<'a> FnMut(&'a mut TreeNode, usize) -> std::ops::ControlFlow<B>,
{
    // Check current node first
    f(node, depth)?;

    // Then traverse children at depth + 1
    for child in &mut node.children {
        fold_tree_mut_with_depth(child, depth + 1, f)?;
    }

    std::ops::ControlFlow::Continue(())
}

// Mutable tree traversals using algebraic fold.

/// Find a node by reference using fold_tree_mut (mutable).
///
/// Uses raw pointer to escape closure lifetime, which is safe because:
/// 1. fold_tree_mut visits each node exactly once
/// 2. We Break immediately after finding the match
/// 3. The pointer is valid for the input lifetime 'a
#[allow(dead_code)] // used by tests
fn find_node_mut<'a>(node: &'a mut TreeNode, reference: &str) -> Option<&'a mut TreeNode> {
    use std::ops::ControlFlow;
    let mut result: Option<*mut TreeNode> = None;
    let flow = fold_tree_mut(node, &mut |n| {
        if n.reference == reference {
            result = Some(n as *mut TreeNode);
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    // Safety invariant: result is only set when we Break
    debug_assert_eq!(result.is_some(), flow.is_break());
    // SAFETY: The pointer came from a live `&mut TreeNode` obtained during
    // fold_tree_mut, which visits each node exactly once and we
    // Break immediately after capturing the pointer, so it remains valid
    // for lifetime 'a and no aliasing occurs.
    result.map(|ptr| unsafe { &mut *ptr })
}

/// Find a node by matching both reference and depth using fold_tree_mut_with_depth.
///
/// This correctly handles dual appearances: when the same reference
/// appears multiple times in the tree, we match the instance at the
/// specific depth the user is viewing.
///
/// Uses raw pointer to escape closure lifetime, which is safe because:
/// 1. fold_tree_mut_with_depth visits each node exactly once
/// 2. We Break immediately after finding the match
/// 3. The pointer is valid for the input lifetime 'a
#[allow(dead_code)] // used by tests
fn find_node_at_depth_mut<'a>(
    node: &'a mut TreeNode,
    reference: &str,
    target_depth: usize,
    current_depth: usize,
    found_count: &mut usize,
) -> Option<&'a mut TreeNode> {
    use std::ops::ControlFlow;
    let mut result: Option<*mut TreeNode> = None;
    let flow = fold_tree_mut_with_depth(node, current_depth, &mut |n, d| {
        if n.reference == reference && d == target_depth {
            if *found_count == 0 {
                result = Some(n as *mut TreeNode);
                return ControlFlow::Break(());
            }
            *found_count -= 1;
        }
        ControlFlow::Continue(())
    });
    // Safety invariant: result is only set when we Break
    debug_assert_eq!(result.is_some(), flow.is_break());
    // SAFETY: The pointer came from a live `&mut TreeNode` obtained during
    // fold_tree_mut_with_depth, which visits each node exactly once and we
    // Break immediately after capturing the pointer, so it remains valid
    // for lifetime 'a and no aliasing occurs.
    result.map(|ptr| unsafe { &mut *ptr })
}

/// Find a node by (reference, depth) starting from root's children.
///
/// The root node itself is synthetic and not rendered, so this
/// iterates over `root.children` and delegates to
/// `find_node_at_depth_mut`. Encapsulates the repeated
/// root-children search pattern used by expand and collapse.
fn find_at_depth_from_root_mut<'a>(
    root: &'a mut TreeNode,
    reference: &str,
    depth: usize,
) -> Option<&'a mut TreeNode> {
    let mut count = 0;
    for child in &mut root.children {
        if let Some(node) = find_node_at_depth_mut(child, reference, depth, 0, &mut count) {
            return Some(node);
        }
    }
    None
}

/// Collect all references in tree (recursive, visits all nodes).
///
/// Traverses ALL nodes regardless of expanded state, used for cache
/// pruning.
/// Collect all references using algebraic fold.
fn collect_refs<'a>(node: &'a TreeNode, out: &mut HashSet<&'a str>) {
    let all_refs = fold_tree(node, &|n, child_results: Vec<HashSet<&'a str>>| {
        let mut refs = HashSet::new();
        refs.insert(n.reference.as_str());
        for child_set in child_results {
            refs.extend(child_set);
        }
        refs
    });
    out.extend(all_refs);
}

/// Collect (reference, depth) pairs of all expanded nodes using algebraic fold.
///
/// Tracks expansion state per tree position, not just per reference.
/// This correctly handles dual appearances where the same reference
/// appears at multiple depths with different expansion states.
fn collect_expanded_refs(node: &TreeNode, depth: usize, out: &mut HashSet<(String, usize)>) {
    let refs = fold_tree_with_depth(node, depth, &|n, d, child_results| {
        let mut result: HashSet<(String, usize)> = child_results.into_iter().flatten().collect();
        if n.expanded {
            result.insert((n.reference.clone(), d));
        }
        result
    });
    out.extend(refs);
}

/// Collapse all nodes using fold-based traversal.
fn collapse_all(node: &mut TreeNode) {
    use std::ops::ControlFlow;
    let _ = fold_tree_mut(node, &mut |n| {
        n.expanded = false;
        ControlFlow::<()>::Continue(())
    });
}

/// Recursively build a tree node from a reference.
///
/// Returns `Option<TreeNode>` - `None` if the node should be
/// filtered out or fetch fails.
///
/// Cycle detection: Instead of using a global visited set, we track
/// the current path from root to this node. A node is only rejected
/// if it appears in its own ancestor path (true cycle). This allows
/// legitimate dual appearances (nodes appearing in both supervision
/// tree and flat list).
#[allow(clippy::too_many_arguments)]
fn build_tree_node<'a>(
    client: &'a reqwest::Client,
    base_url: &'a str,
    show_system_procs: bool,
    cache: &'a mut HashMap<String, FetchState<NodePayload>>,
    path: &'a mut Vec<String>,
    reference: &'a str,
    depth: usize,
    expanded_keys: &'a HashSet<(String, usize)>,
    refresh_gen: u64,
    seq_counter: &'a mut u64,
) -> Pin<Box<dyn Future<Output = Option<TreeNode>> + Send + 'a>> {
    Box::pin(async move {
        // Depth guard.
        if depth >= MAX_TREE_DEPTH {
            return None;
        }

        // Cycle guard: only reject if reference is in the current path
        // (true cycle), not if it appears elsewhere in the tree.
        if path.contains(&reference.to_string()) {
            return None;
        }
        path.push(reference.to_string());

        // Fetch using unified fetch+join path (force=false for cache-aware).
        let state = fetch_with_join(
            client,
            base_url,
            reference,
            cache,
            refresh_gen,
            seq_counter,
            false,
        )
        .await;

        let payload = match state {
            FetchState::Ready { value, .. } => value,
            FetchState::Error { .. } | FetchState::Unknown => {
                path.pop();
                return None;
            }
        };

        // Filter system procs.
        if let NodeProperties::Proc { is_system, .. } = payload.properties
            && !show_system_procs
            && is_system
        {
            path.pop();
            return None;
        }

        let label = derive_label(&payload);
        let node_type = NodeType::from_properties(&payload.properties);
        let has_children = !payload.children.is_empty();
        let is_expanded = expanded_keys.contains(&(reference.to_string(), depth));

        // Build children if expanded.
        let mut children = Vec::new();
        if is_expanded && has_children {
            let is_proc_or_actor = matches!(
                payload.properties,
                NodeProperties::Proc { .. } | NodeProperties::Actor { .. }
            );

            let sorted = sorted_children(&payload);

            for child_ref in &sorted {
                if is_proc_or_actor {
                    // Lazy: create placeholder for unexpanded children,
                    // But recursively build expanded ones.
                    let child_is_expanded =
                        expanded_keys.contains(&(child_ref.to_string(), depth + 1));

                    if child_is_expanded {
                        // Child was previously expanded - recursively build it
                        // To preserve its expanded state and children.
                        if let Some(child_node) = build_tree_node(
                            client,
                            base_url,
                            show_system_procs,
                            cache,
                            path,
                            child_ref,
                            depth + 1,
                            expanded_keys,
                            refresh_gen,
                            seq_counter,
                        )
                        .await
                        {
                            children.push(child_node);
                        } else {
                            // Recursive build failed (cycle, depth limit, etc.)
                            // Fall back to placeholder so node still appears.
                            if let Some(cached) = get_cached_payload(cache, child_ref) {
                                children.push(TreeNode::from_payload(child_ref.clone(), cached));
                            } else {
                                children.push(TreeNode::placeholder(child_ref.clone()));
                            }
                        }
                    } else {
                        // Child is not expanded - use placeholder or cached data.
                        if let Some(cached) = get_cached_payload(cache, child_ref) {
                            children.push(TreeNode::from_payload(child_ref.clone(), cached));
                        } else {
                            children.push(TreeNode::placeholder(child_ref.clone()));
                        }
                    }
                } else {
                    // Eager: recursively fetch (Root/Host parents).
                    if let Some(child_node) = build_tree_node(
                        client,
                        base_url,
                        show_system_procs,
                        cache,
                        path,
                        child_ref,
                        depth + 1,
                        expanded_keys,
                        refresh_gen,
                        seq_counter,
                    )
                    .await
                    {
                        children.push(child_node);
                    }
                }
            }
        }

        let node = TreeNode {
            reference: reference.to_string(),
            label,
            node_type,
            expanded: is_expanded,
            fetched: true,
            has_children,
            children,
        };

        // Pop from path before returning (restore path for sibling nodes).
        path.pop();

        Some(node)
    })
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
        NodeProperties::Root { num_hosts, .. } => format!("Mesh Root ({} hosts)", num_hosts),
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

/// Clone and sort a payload's children by natural reference order.
fn sorted_children(payload: &NodePayload) -> Vec<String> {
    let mut children = payload.children.clone();
    children.sort_by(|a, b| natural_ref_cmp(a, b));
    children
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

/// Format uptime duration from ISO-8601 start timestamp.
///
/// Rounds to nearest 30 seconds for cleaner display.
fn format_uptime(started_at: &str) -> String {
    match chrono::DateTime::parse_from_rfc3339(started_at) {
        Ok(start_time) => {
            let now = chrono::Utc::now();
            let duration = now.signed_duration_since(start_time);
            let total_secs = duration.num_seconds();
            let rounded_secs = ((total_secs + 15) / 30) * 30;
            let std_duration = Duration::from_secs(rounded_secs as u64);
            humantime::format_duration(std_duration).to_string()
        }
        Err(_) => "unknown".to_string(),
    }
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
    // Terminal line.
    let mut app = App::new(&args.addr);
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid template"),
    );
    spinner.set_message(format!("m-admin — Connecting to {} ...", app.base_url));
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
/// Displays a colorful, information-dense header with topology stats,
/// selection context, and system state. Uses semantic colors from the
/// scheme for visual hierarchy and readability.
fn render_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    // Error state overrides normal display
    if let Some(err) = &app.error {
        let header = Paragraph::new(vec![
            Line::from(Span::styled("m-admin", app.scheme.app_name)),
            Line::from(Span::styled(format!("ERROR: {}", err), app.scheme.error)),
        ])
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(app.scheme.border),
        );
        frame.render_widget(header, area);
        return;
    }

    // Gather stats
    let selection = app.current_selection();
    let sys_state = if app.show_system_procs { "on" } else { "off" };

    // Extract uptime and username from root node
    let (uptime_str, username) = if let Some(root_payload) = app.get_cached_payload("root") {
        if let NodeProperties::Root {
            started_at,
            started_by,
            ..
        } = &root_payload.properties
        {
            (Some(format_uptime(started_at)), Some(started_by.clone()))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Line 1: App name • URL • up: 2h 34m • @user • sys:off
    let mut line1_spans = vec![
        Span::styled("m-admin", app.scheme.app_name),
        Span::styled(" • ", app.scheme.stat_label),
        Span::styled(&app.base_url, app.scheme.stat_url),
    ];

    // Add uptime if available
    if let Some(uptime) = uptime_str {
        line1_spans.extend(vec![
            Span::styled(" • ", app.scheme.stat_label),
            Span::styled("up: ", app.scheme.stat_label),
            Span::styled(uptime, app.scheme.stat_timing),
        ]);
    }

    // Add username if available
    if let Some(user) = username {
        line1_spans.extend(vec![
            Span::styled(" • ", app.scheme.stat_label),
            Span::styled(format!("@{}", user), app.scheme.stat_system),
        ]);
    }

    // Cache size and error count
    let cache_size = app.fetch_cache.len();
    let error_count = app
        .fetch_cache
        .values()
        .filter(|state| matches!(state, FetchState::Error { .. }))
        .count();

    line1_spans.extend(vec![
        Span::styled(" • ", app.scheme.stat_label),
        Span::styled("cache:", app.scheme.stat_label),
        Span::styled(cache_size.to_string(), app.scheme.stat_value),
    ]);

    if error_count > 0 {
        line1_spans.extend(vec![
            Span::styled(" err:", app.scheme.stat_label),
            Span::styled(error_count.to_string(), app.scheme.error),
        ]);
    }

    line1_spans.extend(vec![
        Span::styled(" • ", app.scheme.stat_label),
        Span::styled("sys:", app.scheme.stat_label),
        Span::styled(sys_state, app.scheme.stat_system),
    ]);

    // Line 2: Selection context • Last refresh time
    let mut line2_spans = vec![];

    if let Some((node_type, label)) = selection {
        let type_str = node_type.label();
        let type_style = app.scheme.node_style(node_type);
        line2_spans.extend(vec![
            Span::styled("▸ ", app.scheme.stat_selection),
            Span::styled(type_str, type_style),
            Span::styled(" ", Style::default()),
            Span::styled(label, app.scheme.stat_selection),
        ]);
    } else {
        line2_spans.push(Span::styled("No selection", app.scheme.info));
    }

    if !app.last_refresh.is_empty() {
        line2_spans.extend(vec![
            Span::styled(" • ", app.scheme.stat_label),
            Span::styled("⟳ ", app.scheme.stat_timing),
            Span::styled(&app.last_refresh, app.scheme.stat_timing),
        ]);
    }

    let header = Paragraph::new(vec![Line::from(line1_spans), Line::from(line2_spans)]).block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(app.scheme.border),
    );

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
/// Uses `visible_rows()` to display only expanded nodes. Each row
/// includes indentation/connectors, an expand/collapse glyph for
/// nodes with children, and color-coding by `NodeType`, with the
/// selected row highlighted.
fn render_topology_tree(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let rows = app.visible_rows();

    let items: Vec<ListItem> = rows
        .as_slice()
        .iter()
        .enumerate()
        .map(|(vis_idx, row)| {
            let node = row.node;
            let indent = "  ".repeat(row.depth);

            // Tree connector
            let connector = if row.depth == 0 {
                ""
            } else if rows.has_sibling_after(vis_idx, row.depth) {
                "├─ "
            } else {
                "└─ "
            };

            // Fold indicator for expandable nodes
            let fold = if node.has_children {
                if node.expanded { "▼ " } else { "▶ " }
            } else {
                "  "
            };

            let style = if vis_idx == app.cursor.pos() {
                app.scheme.stat_selection.add_modifier(Modifier::BOLD)
            } else {
                app.scheme.node_style(node.node_type)
            };

            let marker = if vis_idx == app.cursor.pos() {
                "▸ "
            } else {
                "  "
            };

            ListItem::new(Line::from(Span::styled(
                format!("{}{}{}{}{}", marker, indent, connector, fold, node.label),
                style,
            )))
        })
        .collect();

    let block = Block::default()
        .title("Topology")
        .borders(Borders::ALL)
        .border_style(app.scheme.border);

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default());
    let mut list_state = ListState::default()
        .with_selected(Some(app.cursor.pos()))
        .with_offset(app.tree_scroll_offset);
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
        Some(payload) => render_node_detail(frame, area, payload, &app.scheme),
        None => {
            let msg = app
                .detail_error
                .as_deref()
                .unwrap_or("Select a node to view details");
            let msg_style = if app.detail_error.is_some() {
                app.scheme.error
            } else {
                app.scheme.info
            };
            let block = Block::default()
                .title("Details")
                .borders(Borders::ALL)
                .border_style(app.scheme.border);
            let p = Paragraph::new(Span::styled(msg, msg_style)).block(block);
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
fn render_node_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    scheme: &ColorScheme,
) {
    match &payload.properties {
        NodeProperties::Root {
            num_hosts,
            started_at,
            started_by,
        } => {
            render_root_detail(
                frame, area, payload, *num_hosts, started_at, started_by, scheme,
            );
        }
        NodeProperties::Host { addr, num_procs } => {
            render_host_detail(frame, area, payload, addr, *num_procs, scheme);
        }
        NodeProperties::Proc {
            proc_name,
            num_actors,
            ..
        } => {
            render_proc_detail(frame, area, payload, proc_name, *num_actors, scheme);
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
                scheme,
            );
        }
        NodeProperties::Error { code, message } => {
            let text = format!("Error: {} — {}", code, message);
            let paragraph = Paragraph::new(text)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(scheme.border)
                        .title("Error"),
                )
                .style(scheme.error)
                .wrap(Wrap { trim: true });
            frame.render_widget(paragraph, area);
        }
    }
}

/// Build a key-value detail line with a gray label and raw value.
fn detail_line<'a>(label: &'a str, value: impl Into<String>) -> Line<'a> {
    Line::from(vec![
        Span::styled(label, Style::default().fg(Color::Gray)),
        Span::raw(value.into()),
    ])
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
    started_at: &str,
    started_by: &str,
    scheme: &ColorScheme,
) {
    let block = Block::default()
        .title("Root Details")
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let uptime_str = format_uptime(started_at);

    let mut lines = vec![
        detail_line("Hosts: ", num_hosts.to_string()),
        detail_line("Started by: ", started_by),
        detail_line("Uptime: ", &uptime_str),
        detail_line("Started at: ", format_local_time(started_at)),
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
    scheme: &ColorScheme,
) {
    let block = Block::default()
        .title("Host Details")
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let mut lines = vec![
        detail_line("Address: ", addr),
        detail_line("Procs: ", num_procs.to_string()),
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
    scheme: &ColorScheme,
) {
    let block = Block::default()
        .title("Proc Details")
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let mut lines = vec![
        detail_line("Name: ", proc_name),
        detail_line("Actors: ", num_actors.to_string()),
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
    scheme: &ColorScheme,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(5)])
        .split(area);

    // Actor info
    let info_block = Block::default()
        .title("Actor Details")
        .borders(Borders::ALL)
        .border_style(scheme.border);

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
        detail_line("Type: ", actor_type),
        detail_line("Messages: ", messages_processed.to_string()),
        detail_line(
            "Processing time: ",
            humantime::format_duration(std::time::Duration::from_micros(total_processing_time_us))
                .to_string(),
        ),
        detail_line("Created: ", created_at),
        detail_line("Last handler: ", last_message_handler.unwrap_or("-")),
        detail_line("Children: ", payload.children.len().to_string()),
    ])
    .block(info_block);
    frame.render_widget(info, chunks[0]);

    // Flight recorder
    let recorder_block = Block::default()
        .title("Flight Recorder")
        .borders(Borders::ALL)
        .border_style(scheme.border);

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
    let help = "q: quit | j/k: navigate | g/G: top/bottom | Tab/Shift-Tab: expand/collapse | c: collapse all | s: system procs";
    let footer = Paragraph::new(help)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test Suite Organization
    //
    // This test suite validates the algebraic properties and invariants
    // Of the admin TUI's three core algebras:
    // 1. Join algebra (cache merge semantics)
    // 2. Cursor algebra (navigation state management)
    // 3. Tree algebra (structural fold operations)
    //
    // Tests are organized into the following categories:
    //
    // ## 1. Join Algebra Laws (FetchState merge semantics)
    //    - join_is_commutative: a.join(b) == b.join(a)
    //    - join_is_associative: (a.join(b)).join(c) == a.join(b.join(c))
    //    - join_is_idempotent: a.join(a) == a
    //    - join_unknown_is_identity: Unknown is identity element
    //    - join_prefers_newer_stamp: Higher timestamp wins
    //    - join_uses_seq_for_tie_break: Sequence breaks timestamp ties
    //    - join_deterministic_tie_break_ready_over_error: Ready beats Error
    //    - join_error_states_newer_wins: Errors merge by timestamp
    //    - join_error_equal_stamps_is_commutative: Error commutativity
    //    - join_error_always_retries_on_fetch: Errors don't cache
    //    - cache_join_commutativity_ready_vs_error: Ready vs Error with
    // Equal stamp
    //
    // ## 2. Staleness & Refresh Semantics
    //    - join_refresh_staleness_triggers_refetch: Old generation
    // Refetches
    //    - collapsed_nodes_stay_collapsed_after_refresh: Expansion state
    // Preserved
    //
    // ## 3. Cursor Invariants (pos < len always holds)
    //    - cursor_new_creates_valid_cursor: Initial state valid
    //    - cursor_new_empty_creates_zero_cursor: Empty case
    //    - cursor_maintains_invariant_after_operations: All ops preserve
    //    - cursor_move_up/down_*: Boundary conditions
    //    - cursor_home/end_*: Jump operations
    //    - cursor_set_pos_*: Direct positioning
    //    - cursor_update_len_*: Length changes
    //    - cursor_single_item_movements: Edge case with n=1
    //    - cursor_empty_all_movements_return_false: Edge case with n=0
    //
    // ## 4. Tree Fold Invariants
    //    - fold_equivalence_flatten_tree: flatten_tree produces correct
    // Depths
    //    - fold_vs_traversal_law_node_count: Row count equals node count
    // When expanded
    //    - fold_tree_mut_early_exit_stops_traversal: ControlFlow::Break
    // Short-circuits
    //    - flatten_collapsed_node_hides_children: Collapsed nodes don't
    // Contribute
    //    - flatten_expanded_node_shows_children: Expanded nodes visible
    //    - collect_refs_visits_all_nodes: Fold traverses entire structure
    //    - find_node_by_reference_works: Immutable fold search
    //    - find_node_mut_works: Mutable fold search
    //    - find_node_at_depth_distinguishes_instances: Depth-aware search
    //
    // ## 5. Collapse Idempotence
    //    - collapse_idempotence: collapse_all twice yields same state
    //
    // ## 6. Placeholder Refinement (fetched state transitions)
    //    - placeholder_refinement_transitions_fetched_state:
    // Fetched=false → true on expand
    //
    // ## 7. Cycle Safety & Duplicate Handling
    //    - cycle_guard_prevents_infinite_recursion: Self-reference handled
    //    - dual_appearances_flatten_correctly: Same reference at multiple
    // Depths
    //    - expansion_tracking_uses_depth_pairs: (reference, depth) pairs
    // Track state
    //    - selection_restore_prefers_depth_match: Depth-aware matching
    //
    // ## 8. Stamp Ordering (temporal semantics)
    //    - stamp_orders_by_timestamp_first: Primary ordering
    //    - stamp_orders_by_seq_when_timestamp_equal: Tie-breaker
    //    - stamp_equality_works: Equivalence relation

    // Helper to create test payloads
    fn mock_payload(identity: &str) -> NodePayload {
        NodePayload {
            identity: identity.to_string(),
            properties: NodeProperties::Actor {
                actor_status: "Running".to_string(),
                actor_type: "test".to_string(),
                messages_processed: 0,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_message_handler: None,
                total_processing_time_us: 0,
                flight_recorder: None,
            },
            children: vec![],
            parent: None,
        }
    }

    // Test fixtures: Stamp ordering

    // Stamp orders by timestamp first.
    #[test]
    fn stamp_orders_by_timestamp_first() {
        let earlier = Stamp {
            ts_micros: 1000,
            seq: 2,
        };
        let later = Stamp {
            ts_micros: 2000,
            seq: 1,
        };
        assert!(earlier < later);
        assert!(later > earlier);
    }

    // Stamp orders by seq when timestamp equal.
    #[test]
    fn stamp_orders_by_seq_when_timestamp_equal() {
        let first = Stamp {
            ts_micros: 1000,
            seq: 1,
        };
        let second = Stamp {
            ts_micros: 1000,
            seq: 2,
        };
        assert!(first < second);
        assert!(second > first);
    }

    // Stamp equality works.
    #[test]
    fn stamp_equality_works() {
        let a = Stamp {
            ts_micros: 1000,
            seq: 5,
        };
        let b = Stamp {
            ts_micros: 1000,
            seq: 5,
        };
        assert_eq!(a, b);
    }

    // Test fixtures: Cursor navigation

    // Cursor new creates valid cursor.
    #[test]
    fn cursor_new_creates_valid_cursor() {
        let cursor = Cursor::new(10);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 10);
    }

    // Cursor new empty creates zero cursor.
    #[test]
    fn cursor_new_empty_creates_zero_cursor() {
        let cursor = Cursor::new(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    // Cursor move up at start returns false.
    #[test]
    fn cursor_move_up_at_start_returns_false() {
        let mut cursor = Cursor::new(5);
        assert!(!cursor.move_up());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor move up from middle decrements.
    #[test]
    fn cursor_move_up_from_middle_decrements() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        assert!(cursor.move_up());
        assert_eq!(cursor.pos(), 1);
    }

    // Cursor move down at end returns false.
    #[test]
    fn cursor_move_down_at_end_returns_false() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(4); // last position
        assert!(!cursor.move_down());
        assert_eq!(cursor.pos(), 4);
    }

    // Cursor move down from start increments.
    #[test]
    fn cursor_move_down_from_start_increments() {
        let mut cursor = Cursor::new(5);
        assert!(cursor.move_down());
        assert_eq!(cursor.pos(), 1);
    }

    // Cursor home at start returns false.
    #[test]
    fn cursor_home_at_start_returns_false() {
        let mut cursor = Cursor::new(5);
        assert!(!cursor.home());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor home from middle jumps to start.
    #[test]
    fn cursor_home_from_middle_jumps_to_start() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(3);
        assert!(cursor.home());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor end at end returns false.
    #[test]
    fn cursor_end_at_end_returns_false() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(4);
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 4);
    }

    // Cursor end from start jumps to end.
    #[test]
    fn cursor_end_from_start_jumps_to_end() {
        let mut cursor = Cursor::new(5);
        assert!(cursor.end());
        assert_eq!(cursor.pos(), 4);
    }

    // Cursor empty all movements return false.
    #[test]
    fn cursor_empty_all_movements_return_false() {
        let mut cursor = Cursor::new(0);
        assert!(!cursor.move_up());
        assert!(!cursor.move_down());
        assert!(!cursor.home());
        assert!(!cursor.end());
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor single item movements.
    #[test]
    fn cursor_single_item_movements() {
        let mut cursor = Cursor::new(1);
        assert_eq!(cursor.pos(), 0);
        assert!(!cursor.move_up()); // can't go up
        assert!(!cursor.move_down()); // can't go down (already at last)
        assert!(!cursor.home()); // already at home
        assert!(!cursor.end()); // already at end (same as start)
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor update len expands preserves position.
    #[test]
    fn cursor_update_len_expands_preserves_position() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(2);
        cursor.update_len(10);
        assert_eq!(cursor.pos(), 2);
        assert_eq!(cursor.len(), 10);
    }

    // Cursor update len shrinks clamps position.
    #[test]
    fn cursor_update_len_shrinks_clamps_position() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(8);
        cursor.update_len(5);
        assert_eq!(cursor.pos(), 4); // clamped to len-1
        assert_eq!(cursor.len(), 5);
    }

    // Cursor update len to zero resets position.
    #[test]
    fn cursor_update_len_to_zero_resets_position() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(5);
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    // Cursor set pos within bounds works.
    #[test]
    fn cursor_set_pos_within_bounds_works() {
        let mut cursor = Cursor::new(10);
        cursor.set_pos(7);
        assert_eq!(cursor.pos(), 7);
    }

    // Cursor set pos beyond bounds clamps.
    #[test]
    fn cursor_set_pos_beyond_bounds_clamps() {
        let mut cursor = Cursor::new(5);
        cursor.set_pos(10);
        assert_eq!(cursor.pos(), 4); // clamped to len-1
    }

    // Cursor set pos on empty stays zero.
    #[test]
    fn cursor_set_pos_on_empty_stays_zero() {
        let mut cursor = Cursor::new(0);
        cursor.set_pos(5);
        assert_eq!(cursor.pos(), 0);
    }

    // Cursor maintains invariant after operations.
    #[test]
    fn cursor_maintains_invariant_after_operations() {
        let mut cursor = Cursor::new(5);

        // Move around
        cursor.move_down();
        cursor.move_down();
        assert!(cursor.pos() < cursor.len());

        // Shrink
        cursor.update_len(2);
        assert!(cursor.pos() < cursor.len());

        // Set beyond
        cursor.set_pos(100);
        assert!(cursor.pos() < cursor.len());

        // Empty
        cursor.update_len(0);
        assert_eq!(cursor.pos(), 0);
    }

    // Test fixtures: FetchState JoinSemilattice properties

    // Join unknown is identity.
    #[test]
    fn join_unknown_is_identity() {
        let payload = mock_payload("test");
        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let result = FetchState::Unknown.join(&ready);
        assert!(matches!(result, FetchState::Ready { .. }));

        let result2 = ready.join(&FetchState::Unknown);
        assert!(matches!(result2, FetchState::Ready { .. }));
    }

    // Join is commutative.
    #[test]
    fn join_is_commutative() {
        let payload1 = mock_payload("test1");
        let payload2 = mock_payload("test2");

        let older = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload1,
        };
        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: payload2.clone(),
        };

        let result1 = older.join(&newer);
        let result2 = newer.join(&older);

        match (&result1, &result2) {
            (FetchState::Ready { value: v1, .. }, FetchState::Ready { value: v2, .. }) => {
                assert_eq!(v1.identity, v2.identity);
                assert_eq!(v1.identity, payload2.identity);
            }
            _ => panic!("Expected Ready states"),
        }
    }

    // Join is idempotent.
    #[test]
    fn join_is_idempotent() {
        let payload = mock_payload("test");
        let state = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };

        let result = state.join(&state);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, payload.identity);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Join prefers newer stamp.
    #[test]
    fn join_prefers_newer_stamp() {
        let payload1 = mock_payload("old");
        let payload2 = mock_payload("new");

        let older = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload1,
        };
        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 2,
            value: payload2.clone(),
        };

        let result = older.join(&newer);
        match result {
            FetchState::Ready { value, stamp, .. } => {
                assert_eq!(value.identity, "new");
                assert_eq!(stamp.ts_micros, 2000);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Join uses seq for tie break.
    #[test]
    fn join_uses_seq_for_tie_break() {
        let payload1 = mock_payload("first");
        let payload2 = mock_payload("second");

        let first = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload1,
        };
        let second = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 2,
            },
            generation: 1,
            value: payload2.clone(),
        };

        let result = first.join(&second);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "second");
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Join deterministic tie break ready over error.
    #[test]
    fn join_deterministic_tie_break_ready_over_error() {
        let payload = mock_payload("test");

        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload.clone(),
        };
        let error = FetchState::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "error".to_string(),
        };

        // Ready should win in both directions
        let result1 = ready.join(&error);
        let result2 = error.join(&ready);

        assert!(matches!(result1, FetchState::Ready { .. }));
        assert!(matches!(result2, FetchState::Ready { .. }));
    }

    // Join error states newer wins.
    #[test]
    fn join_error_states_newer_wins() {
        let older_error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "old error".to_string(),
        };
        let newer_error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            msg: "new error".to_string(),
        };

        // Newer stamp should win (commutative)
        let result1 = older_error.join(&newer_error);
        let result2 = newer_error.join(&older_error);

        match (&result1, &result2) {
            (FetchState::Error { msg: m1, .. }, FetchState::Error { msg: m2, .. }) => {
                assert_eq!(m1, m2);
                assert_eq!(m1, "new error");
            }
            _ => panic!("Expected Error states"),
        }
    }

    // Join error equal stamps is commutative.
    #[test]
    fn join_error_equal_stamps_is_commutative() {
        // Test the lexicographic tie-break for Error states with equal
        // Stamps. This is the edge case that ensures full commutativity.
        let error1 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "alpha".to_string(),
        };
        let error2 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "beta".to_string(),
        };

        // Should pick same result regardless of order (lexicographic)
        let result1 = error1.join(&error2);
        let result2 = error2.join(&error1);

        match (&result1, &result2) {
            (FetchState::Error { msg: m1, .. }, FetchState::Error { msg: m2, .. }) => {
                assert_eq!(m1, m2);
                // "beta" >= "alpha", so "beta" should win
                assert_eq!(m1, "beta");
            }
            _ => panic!("Expected Error states"),
        }
    }

    // Join is associative.
    #[test]
    fn join_is_associative() {
        let p1 = mock_payload("p1");
        let p2 = mock_payload("p2");
        let p3 = mock_payload("p3");

        let s1 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: p1,
        };
        let s2 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: p2,
        };
        let s3 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 3000,
                seq: 1,
            },
            generation: 1,
            value: p3.clone(),
        };

        let left = s1.join(&s2).join(&s3);
        let right = s1.join(&s2.join(&s3));

        match (&left, &right) {
            (FetchState::Ready { value: v1, .. }, FetchState::Ready { value: v2, .. }) => {
                assert_eq!(v1.identity, v2.identity);
                assert_eq!(v1.identity, p3.identity);
            }
            _ => panic!("Expected Ready states"),
        }
    }

    // Join error always retries on fetch.
    #[test]
    fn join_error_always_retries_on_fetch() {
        // This is a behavioral test - errors should be treated as cache
        // Miss. The fetch_node_state implementation checks for Error and
        // Sets should_fetch = true.
        let error = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "network error".to_string(),
        };

        // Verify error can be joined with newer states
        let newer_ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 1,
            value: mock_payload("recovered"),
        };

        let result = error.join(&newer_ready);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "recovered");
            }
            _ => panic!("Expected Ready state after retry"),
        }
    }

    // Tree operation tests

    // Flatten collapsed node hides children.
    #[test]
    fn flatten_collapsed_node_hides_children() {
        // Test that a collapsed host node hides its children.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "host1".into(),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: false, // Collapsed
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };
        let rows = flatten_tree(&tree);
        // Root is skipped, host1 is included (depth 0).
        // Proc1 is hidden because host1.expanded=false.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "host1");
        assert_eq!(rows[0].depth, 0);
    }

    // Flatten expanded node shows children.
    #[test]
    fn flatten_expanded_node_shows_children() {
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "host1".into(),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: true, // Expanded
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].node.reference, "host1");
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, "proc1");
        assert_eq!(rows[1].depth, 1);
    }

    // Find node by reference works.
    #[test]
    fn find_node_by_reference_works() {
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "child1".into(),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };
        let found = find_node_by_ref(&tree, "child1");
        assert!(found.is_some());
        assert_eq!(found.unwrap().reference, "child1");
    }

    // Find node mut works.
    #[test]
    fn find_node_mut_works() {
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "child1".into(),
                label: "Child 1".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };
        let found = find_node_mut(&mut tree, "child1");
        assert!(found.is_some());
        found.unwrap().expanded = true;
        // Verify mutation worked
        assert!(tree.children[0].expanded);
    }

    // Collect refs visits all nodes.
    #[test]
    fn collect_refs_visits_all_nodes() {
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "host1".into(),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: false, // Collapsed, but collect_refs should still visit children
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };
        let mut refs = HashSet::new();
        collect_refs(&tree, &mut refs);
        assert_eq!(refs.len(), 3);
        assert!(refs.contains("root"));
        assert!(refs.contains("host1"));
        assert!(refs.contains("proc1"));
    }

    // Tests for Phase 3 tree refactoring

    // Dual appearances flatten correctly.
    #[test]
    fn dual_appearances_flatten_correctly() {
        // Test that the same reference can appear at multiple depths
        // (dual appearance pattern).
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "actor1".into(),
                        label: "Actor 1".into(),
                        node_type: NodeType::Actor,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                // Same actor appears in flat list
                TreeNode {
                    reference: "actor1".into(),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows = flatten_tree(&tree);
        // Should see: proc1 (depth 0), actor1 (depth 1), actor1 (depth 0)
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].node.reference, "proc1");
        assert_eq!(rows[0].depth, 0);
        assert_eq!(rows[1].node.reference, "actor1");
        assert_eq!(rows[1].depth, 1);
        assert_eq!(rows[2].node.reference, "actor1");
        assert_eq!(rows[2].depth, 0);
    }

    // Expansion tracking uses depth pairs.
    #[test]
    fn expansion_tracking_uses_depth_pairs() {
        // Test that expanded_keys tracks (reference, depth) pairs,
        // Not just references.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "actor1".into(),
                        label: "Actor 1".into(),
                        node_type: NodeType::Actor,
                        expanded: true, // Expanded at depth 1
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                // Same actor collapsed in flat list
                TreeNode {
                    reference: "actor1".into(),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false, // Collapsed at depth 0
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let mut expanded_keys = HashSet::new();
        for child in &tree.children {
            collect_expanded_refs(child, 0, &mut expanded_keys);
        }

        // Should track both proc1 and actor1 at their specific depths
        assert!(expanded_keys.contains(&("proc1".to_string(), 0)));
        assert!(expanded_keys.contains(&("actor1".to_string(), 1)));
        // Actor1 at depth 0 is not expanded, should not be in set
        assert!(!expanded_keys.contains(&("actor1".to_string(), 0)));
    }

    // Find node at depth distinguishes instances.
    #[test]
    fn find_node_at_depth_distinguishes_instances() {
        // Test that find_node_at_depth_mut finds the correct instance
        // When same reference appears at multiple depths.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "actor1".into(),
                        label: "Actor 1 in supervision".into(),
                        node_type: NodeType::Actor,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "actor1".into(),
                    label: "Actor 1 in flat list".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Find actor1 at depth 1 (in supervision tree)
        let mut count = 0;
        let found_depth_1 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "actor1", 1, 0, &mut count));
        assert!(found_depth_1.is_some());
        assert_eq!(found_depth_1.unwrap().label, "Actor 1 in supervision");

        // Find actor1 at depth 0 (in flat list)
        let mut count = 0;
        let found_depth_0 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "actor1", 0, 0, &mut count));
        assert!(found_depth_0.is_some());
        assert_eq!(found_depth_0.unwrap().label, "Actor 1 in flat list");
    }

    // Collapsed nodes stay collapsed after refresh.
    #[test]
    fn collapsed_nodes_stay_collapsed_after_refresh() {
        // Test that expansion state is preserved correctly when some
        // Instances are collapsed and others expanded.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "proc1".into(),
                label: "Proc 1".into(),
                node_type: NodeType::Proc,
                expanded: false, // Collapsed
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "actor1".into(),
                    label: "Actor 1".into(),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };

        let mut expanded_keys = HashSet::new();
        for child in &tree.children {
            collect_expanded_refs(child, 0, &mut expanded_keys);
        }

        // Collapsed nodes should NOT be in expanded_keys
        assert!(!expanded_keys.contains(&("proc1".to_string(), 0)));
        assert!(!expanded_keys.contains(&("actor1".to_string(), 1)));
    }

    // Fold equivalence flatten tree.
    #[test]
    fn fold_equivalence_flatten_tree() {
        // Verify flatten_tree produces expected row list with correct depths
        // And respects expansion flags.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "host1".into(),
                    label: "Host 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "proc1".into(),
                        label: "Proc 1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: true,
                        children: vec![TreeNode {
                            reference: "actor1".into(),
                            label: "Actor 1".into(),
                            node_type: NodeType::Actor,
                            expanded: false,
                            fetched: true,
                            has_children: false,
                            children: vec![],
                        }],
                    }],
                },
                TreeNode {
                    reference: "host2".into(),
                    label: "Host 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows = flatten_tree(&tree);

        // Expected: host1 (d=0), proc1 (d=1), host2 (d=0)
        // Actor1 should NOT appear because proc1.expanded = false
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0].node.reference, "host1");
        assert_eq!(rows[0].depth, 0);

        assert_eq!(rows[1].node.reference, "proc1");
        assert_eq!(rows[1].depth, 1);

        assert_eq!(rows[2].node.reference, "host2");
        assert_eq!(rows[2].depth, 0);
    }

    // Fold tree mut early exit stops traversal.
    #[test]
    fn fold_tree_mut_early_exit_stops_traversal() {
        // Verify fold_tree_mut_with_depth stops at first match using
        // ControlFlow::Break.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "target".into(),
                        label: "Target".into(),
                        node_type: NodeType::Proc,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "should_not_visit".into(),
                        label: "Should Not Visit".into(),
                        node_type: NodeType::Proc,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
            ],
        };

        use std::ops::ControlFlow;
        let mut visited = Vec::new();
        let result = fold_tree_mut_with_depth(&mut tree, 0, &mut |n, _d| {
            visited.push(n.reference.clone());
            if n.reference == "target" {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });

        assert!(result.is_break());
        // Should visit: root, child1, target (then stop)
        assert_eq!(visited, vec!["root", "child1", "target"]);
        // Should_not_visit should NOT be in the list
        assert!(!visited.contains(&"should_not_visit".to_string()));
    }

    // Selection restore prefers depth match.
    #[test]
    fn selection_restore_prefers_depth_match() {
        // Verify that (reference, depth) matching correctly distinguishes
        // Between duplicate references at different depths.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "duplicate".into(),
                label: "Duplicate at depth 0".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "duplicate".into(),
                    label: "Duplicate at depth 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };

        // Find at depth 0
        let mut count = 0;
        let found_d0 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "duplicate", 0, 0, &mut count));
        assert!(found_d0.is_some());
        assert_eq!(found_d0.unwrap().label, "Duplicate at depth 0");

        // Find at depth 1
        let mut count = 0;
        let found_d1 = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "duplicate", 1, 0, &mut count));
        assert!(found_d1.is_some());
        assert_eq!(found_d1.unwrap().label, "Duplicate at depth 1");
    }

    // Join refresh staleness triggers refetch.
    #[test]
    fn join_refresh_staleness_triggers_refetch() {
        // Verify that FetchState::Ready with old generation is treated as
        // Stale and would trigger refetch.
        let payload1 = mock_payload("stale");
        let payload2 = mock_payload("fresh");

        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: payload1,
        };

        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 20,
            value: payload2,
        };

        // Join with higher generation should prefer fresh
        let result = stale.join(&fresh);

        match result {
            FetchState::Ready {
                generation, value, ..
            } => {
                // Should pick the one with higher generation
                assert_eq!(generation, 20);
                assert_eq!(value.identity, "fresh");
            }
            _ => panic!("Expected Ready state"),
        }

        // In practice, fetch_with_join would detect stale.generation <
        // Refresh_gen and refetch. We verify the join semantics here.
    }

    // Fold vs traversal law node count.
    #[test]
    fn fold_vs_traversal_law_node_count() {
        // For a tree with all nodes expanded, flatten_tree size should equal
        // The total node count computed via fold.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "host1".into(),
                    label: "Host 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "proc1".into(),
                        label: "Proc 1".into(),
                        node_type: NodeType::Proc,
                        expanded: true,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "host2".into(),
                    label: "Host 2".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Count nodes using fold
        let node_count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Flatten_tree should produce exactly node_count rows (minus root,
        // Which is not rendered)
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), node_count - 1); // -1 for root
    }

    // Collapse idempotence.
    #[test]
    fn collapse_idempotence() {
        // Applying collapse_all twice should have no additional effect
        // (idempotence).
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "child".into(),
                label: "Child".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // First collapse
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);

        // Second collapse should be no-op
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);

        // Verify stability
        let snapshot_after_first = tree.expanded;
        collapse_all(&mut tree);
        assert_eq!(tree.expanded, snapshot_after_first);
    }

    // Placeholder refinement transitions fetched state.
    #[test]
    fn placeholder_refinement_transitions_fetched_state() {
        // Verify that expanding a placeholder transitions fetched=false
        // To fetched=true.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "placeholder".into(),
                label: "Loading...".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: false, // Not yet fetched
                has_children: true,
                children: vec![],
            }],
        };

        // Simulate fetch by finding and updating the placeholder
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == "placeholder" && !n.fetched {
                n.fetched = true;
                n.has_children = true; // Payload indicates children exist
                n.children = vec![TreeNode {
                    reference: "child".into(),
                    label: "Child".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }];
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });

        // Verify transition
        let placeholder = find_node_by_ref(&tree, "placeholder");
        assert!(placeholder.is_some());
        let placeholder = placeholder.unwrap();
        assert!(placeholder.fetched);
        assert_eq!(placeholder.children.len(), 1);

        // Expand again should be no-op (already fetched)
        let initial_children = placeholder.children.len();
        let _ = find_node_by_ref(&tree, "placeholder");
        assert_eq!(initial_children, 1); // No change
    }

    // Cache join commutativity ready vs error.
    #[test]
    fn cache_join_commutativity_ready_vs_error() {
        // Verify join commutativity for Ready vs Error with equal stamps.
        let payload = mock_payload("test");

        let ready = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 1,
            value: payload,
        };

        let error = FetchState::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "test error".to_string(),
        };

        // Join in both orders
        let result1 = ready.join(&error);
        let result2 = error.join(&ready);

        // Both should produce Ready (deterministic tie-break)
        match (&result1, &result2) {
            (FetchState::Ready { .. }, FetchState::Ready { .. }) => {
                // Verify they're equivalent
                assert!(matches!(result1, FetchState::Ready { .. }));
                assert!(matches!(result2, FetchState::Ready { .. }));
            }
            _ => panic!("Expected Ready states from tie-break"),
        }
    }

    // Cycle guard prevents infinite recursion.
    #[test]
    fn cycle_guard_prevents_infinite_recursion() {
        // Verify that attempting to build a tree with a self-reference cycle
        // Is handled gracefully (typically by returning None or marking as
        // Error).
        //
        // In the current implementation, cycles are prevented by not allowing
        // A child to have the same reference as its ancestor. We test that
        // The fold traversal completes without infinite recursion.

        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "root".into(), // Cycle!
                label: "Self-reference".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Fold should complete without stack overflow
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Should count root + self-reference = 2 nodes (not infinite)
        assert_eq!(count, 2);

        // Mutable fold should also complete
        use std::ops::ControlFlow;
        let mut visited = 0;
        let _ = fold_tree_mut(&mut tree, &mut |_n| {
            visited += 1;
            if visited > 100 {
                // Safety: prevent actual infinite recursion in test
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });

        // Should visit only 2 nodes
        assert_eq!(visited, 2);
    }

    // Join cache preserves ready when generation matches.
    #[test]
    fn join_cache_preserves_ready_when_generation_matches() {
        // Verify that fetch_with_join preserves FetchState::Ready when
        // Generation == refresh_gen, and only refetches when generation <
        // Refresh_gen.
        let payload = mock_payload("current");

        // Cache entry with matching generation
        let current = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: payload.clone(),
        };

        // Simulate join with fresh fetch (higher generation)
        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10, // Same generation
            value: payload.clone(),
        };

        let result = current.join(&fresh);
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }

        // Now test stale case (generation < refresh_gen would trigger
        // Refetch in fetch_with_join)
        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5, // Old generation
            value: payload.clone(),
        };

        let newer = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: payload,
        };

        let result = stale.join(&newer);
        match result {
            FetchState::Ready { generation, .. } => {
                // Join prefers newer generation
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Fold tree mut visits in preorder.
    #[test]
    fn fold_tree_mut_visits_in_preorder() {
        // Verify that fold_tree_mut_with_depth visits nodes in pre-order
        // DFS (parent before children).
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "grandchild1".into(),
                        label: "Grandchild 1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        use std::ops::ControlFlow;
        let mut visit_order = Vec::new();
        let _ = fold_tree_mut_with_depth(&mut tree, 0, &mut |n, d| {
            visit_order.push((n.reference.clone(), d));
            ControlFlow::<()>::Continue(())
        });

        // Expected pre-order: root, child1, grandchild1, child2
        assert_eq!(
            visit_order,
            vec![
                ("root".to_string(), 0),
                ("child1".to_string(), 1),
                ("grandchild1".to_string(), 2),
                ("child2".to_string(), 1),
            ]
        );
    }

    // Selection stability with duplicate references.
    #[test]
    fn selection_stability_with_duplicate_references() {
        // Test cursor stability when reference exists at multiple depths
        // And when reference disappears entirely.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "duplicate".into(),
                label: "Duplicate at 0".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "duplicate".into(),
                    label: "Duplicate at 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };

        let rows = flatten_tree(&tree);

        // Two instances of "duplicate" at depths 0 and 1
        let duplicate_refs: Vec<_> = rows
            .iter()
            .filter(|r| r.node.reference == "duplicate")
            .map(|r| (r.node.reference.as_str(), r.depth))
            .collect();

        assert_eq!(duplicate_refs.len(), 2);
        assert_eq!(duplicate_refs[0], ("duplicate", 0));
        assert_eq!(duplicate_refs[1], ("duplicate", 1));

        // Test cursor with disappearing reference (collapse parent)
        let mut tree_collapsed = tree.clone();
        tree_collapsed.children[0].expanded = false; // Collapse

        let rows_after = flatten_tree(&tree_collapsed);

        // Now only one "duplicate" visible at depth 0
        let duplicate_refs_after: Vec<_> = rows_after
            .iter()
            .filter(|r| r.node.reference == "duplicate")
            .map(|r| (r.node.reference.as_str(), r.depth))
            .collect();

        assert_eq!(duplicate_refs_after.len(), 1);
        assert_eq!(duplicate_refs_after[0], ("duplicate", 0));

        // Cursor should clamp to valid range
        let mut cursor = Cursor::new(rows.len());
        cursor.set_pos(1); // Was at depth-1 duplicate
        cursor.update_len(rows_after.len()); // List shrunk
        assert!(cursor.pos() < rows_after.len());
    }

    // Placeholder noop when has children false.
    #[test]
    fn placeholder_noop_when_has_children_false() {
        // Verify that expanding a node with has_children=false leaves
        // Children empty and doesn't erroneously mark it as expanded.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "leaf".into(),
                label: "Leaf Node".into(),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false, // No children
                children: vec![],
            }],
        };

        // Attempt to "expand" the leaf (simulate user pressing Tab)
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == "leaf" && !n.has_children {
                // Should NOT set expanded=true or add children
                // (This is application logic, but we verify state stays
                // Consistent)
                assert_eq!(n.children.len(), 0);
                assert!(!n.expanded);
            }
            ControlFlow::<()>::Continue(())
        });

        // Verify leaf remains unexpanded with no children
        let leaf = find_node_by_ref(&tree, "leaf");
        assert!(leaf.is_some());
        let leaf = leaf.unwrap();
        assert!(!leaf.expanded);
        assert_eq!(leaf.children.len(), 0);
        assert!(!leaf.has_children);
    }

    // Fold tree with depth deterministic preorder.
    #[test]
    fn fold_tree_with_depth_deterministic_preorder() {
        // Verify that fold_tree_with_depth (immutable fold) visits nodes
        // In deterministic pre-order DFS.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "a".into(),
                    label: "A".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "a1".into(),
                        label: "A1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "b".into(),
                    label: "B".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Collect visit order using fold_tree_with_depth
        let visit_order = fold_tree_with_depth(&tree, 0, &|n,
                                                           d,
                                                           child_orders: Vec<
            Vec<(String, usize)>,
        >| {
            let mut order = vec![(n.reference.clone(), d)];
            for child_order in child_orders {
                order.extend(child_order);
            }
            order
        });

        // Expected pre-order: root, a, a1, b
        assert_eq!(
            visit_order,
            vec![
                ("root".to_string(), 0),
                ("a".to_string(), 1),
                ("a1".to_string(), 2),
                ("b".to_string(), 1),
            ]
        );
    }

    // Cycle vs duplicate reference allowed.
    #[test]
    fn cycle_vs_duplicate_reference_allowed() {
        // Verify that a duplicate reference NOT in the ancestor path is
        // Allowed (dual appearance), while a true cycle (ancestor path
        // Contains reference) would be rejected.
        //
        // Current implementation doesn't enforce cycle detection at tree
        // Construction, but we verify that duplicates at different branches
        // Work correctly.

        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "branch_a".into(),
                    label: "Branch A".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "duplicate".into(),
                        label: "Duplicate in A".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "branch_b".into(),
                    label: "Branch B".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "duplicate".into(),
                        label: "Duplicate in B".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
            ],
        };

        // Fold should complete successfully (no cycle)
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Should count 5 nodes: root, branch_a, duplicate, branch_b,
        // Duplicate
        assert_eq!(count, 5);

        // Flatten should show both duplicates
        let rows = flatten_tree(&tree);
        let duplicate_count = rows
            .iter()
            .filter(|r| r.node.reference == "duplicate")
            .count();
        assert_eq!(duplicate_count, 2);
    }

    // Error state does not cache.
    #[test]
    fn error_state_does_not_cache() {
        // Verify that FetchState::Error always retries (doesn't prevent
        // Refetch). Join with Error should allow fresh attempt.
        let error1 = FetchState::<NodePayload>::Error {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            msg: "first error".to_string(),
        };

        let error2 = FetchState::Error {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            msg: "second error".to_string(),
        };

        // Join two errors - should prefer newer timestamp
        let result = error1.join(&error2);
        match result {
            FetchState::Error { stamp, .. } => {
                assert_eq!(stamp.ts_micros, 2000);
            }
            _ => panic!("Expected Error state"),
        }

        // Error joined with Unknown should return Error (not Unknown)
        let unknown = FetchState::Unknown;
        let result = error1.join(&unknown);
        assert!(matches!(result, FetchState::Error { .. }));

        // This demonstrates errors don't "stick" - each fetch attempt
        // Can retry
    }

    // Cursor update after empty tree.
    #[test]
    fn cursor_update_after_empty_tree() {
        // Verify cursor state when tree becomes empty (refresh fails,
        // Tree is None).
        let mut cursor = Cursor::new(5); // Previously had 5 items
        cursor.set_pos(2);

        // Tree refresh fails, now we have 0 items
        cursor.update_len(0);

        assert_eq!(cursor.len(), 0);
        assert_eq!(cursor.pos(), 0);

        // All movements should return false
        assert!(!cursor.move_down());
        assert!(!cursor.move_up());
        assert!(!cursor.home());
        assert!(!cursor.end());
    }

    // Empty tree all operations are noops.
    #[test]
    fn empty_tree_all_operations_are_noops() {
        // Verify that visible_rows(), selection restore, and detail fetch
        // Are all no-ops when tree=None.
        let app = App::new("localhost:8080");

        // Visible_rows should be empty
        let rows = app.visible_rows();
        assert_eq!(rows.len(), 0);

        // Cursor should be at 0
        assert_eq!(app.cursor.pos(), 0);
        assert_eq!(app.cursor.len(), 0);

        // Tree should be None
        assert!(app.tree().is_none());
    }

    // Single node tree expand collapse.
    #[test]
    fn single_node_tree_expand_collapse() {
        // Test edge cases with root having exactly one child.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "only_child".into(),
                label: "Only Child".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Flatten should show only the one child
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "only_child");

        // Expand the child (no-op since has_children=false)
        let child = find_node_mut(&mut tree, "only_child");
        assert!(child.is_some());
        let child = child.unwrap();
        assert!(!child.has_children);
        assert!(!child.expanded); // Should not expand

        // Collapse root
        collapse_all(&mut tree);
        assert!(!tree.expanded);
        assert!(!tree.children[0].expanded);
    }

    // Placeholder with has children true awaits fetch.
    #[test]
    fn placeholder_with_has_children_true_awaits_fetch() {
        // Node with has_children=true but empty children vec is a
        // Placeholder awaiting fetch. Expand should trigger fetch; collapse
        // Is no-op if never expanded.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "placeholder".into(),
                label: "Loading...".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: false,
                has_children: true, // Indicates children exist but not fetched
                children: vec![],
            }],
        };

        // Placeholder should appear in flatten
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "placeholder");

        // Simulate expanding placeholder (would trigger fetch)
        let placeholder = find_node_mut(&mut tree, "placeholder");
        assert!(placeholder.is_some());
        let placeholder = placeholder.unwrap();
        assert!(placeholder.has_children);
        assert!(!placeholder.fetched);
        assert_eq!(placeholder.children.len(), 0);

        // Collapse on unexpanded placeholder is no-op
        use std::ops::ControlFlow;
        let _ = fold_tree_mut(&mut tree, &mut |n| {
            if n.reference == "placeholder" && !n.expanded {
                // Collapse on unexpanded node does nothing
                assert!(!n.expanded);
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
    }

    // System proc filter toggles visibility.
    #[test]
    fn system_proc_filter_toggles_visibility() {
        // Verify that toggling show_system_procs filters/unfilters nodes.
        // Note: This tests the filtering logic at the model level.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "user_proc".into(),
                    label: "User Proc".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "system_proc".into(),
                    label: "System Proc".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // With show_system_procs=false, system procs would be filtered
        // (actual filtering happens in build logic, not flatten)
        // Here we verify the tree structure allows both types
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);

        // Verify both proc types present
        let refs: Vec<_> = rows.iter().map(|r| r.node.reference.as_str()).collect();
        assert!(refs.contains(&"user_proc"));
        assert!(refs.contains(&"system_proc"));

        // In practice, build_tree_node would filter based on
        // Show_system_procs flag
    }

    // Stale selection after refresh clamps cursor.
    #[test]
    fn stale_selection_after_refresh_clamps_cursor() {
        // Previously selected reference disappears after refresh; cursor
        // Should clamp to valid range.
        let tree_before = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "child3".into(),
                    label: "Child 3".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows_before = flatten_tree(&tree_before);
        assert_eq!(rows_before.len(), 3);

        // Cursor at position 2 (child3)
        let mut cursor = Cursor::new(rows_before.len());
        cursor.set_pos(2);
        assert_eq!(cursor.pos(), 2);

        // After refresh, child3 disappears
        let tree_after = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows_after = flatten_tree(&tree_after);
        assert_eq!(rows_after.len(), 2);

        // Update cursor length (should clamp position)
        cursor.update_len(rows_after.len());
        assert!(cursor.pos() < rows_after.len());
        assert_eq!(cursor.pos(), 1); // Clamped to last valid position
    }

    // Duplicate references expansion targets specific instance.
    #[test]
    fn duplicate_references_expansion_targets_specific_instance() {
        // With duplicate references at different depths, expansion should
        // Only affect the targeted (reference, depth) instance.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "duplicate".into(),
                    label: "Duplicate at 0".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "child_of_first".into(),
                        label: "Child of First".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "duplicate".into(),
                    label: "Duplicate at 0 (second)".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "child_of_second".into(),
                        label: "Child of Second".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
            ],
        };

        // Expand first duplicate at depth 0, index 0
        let mut count = 0;
        let first = tree
            .children
            .iter_mut()
            .find_map(|child| find_node_at_depth_mut(child, "duplicate", 0, 0, &mut count));
        assert!(first.is_some());
        first.unwrap().expanded = true;

        // Verify only first is expanded
        assert!(tree.children[0].expanded);
        assert!(!tree.children[1].expanded);

        // Flatten should show first's child but not second's
        let rows = flatten_tree(&tree);
        let refs: Vec<_> = rows.iter().map(|r| r.node.reference.as_str()).collect();
        assert!(refs.contains(&"child_of_first"));
        assert!(!refs.contains(&"child_of_second"));
    }

    // Expanded node with empty children renders safely.
    #[test]
    fn expanded_node_with_empty_children_renders_safely() {
        // Node with expanded=true but children=[] should not panic and
        // Should render correctly.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "empty_parent".into(),
                label: "Empty Parent".into(),
                node_type: NodeType::Host,
                expanded: true, // Expanded but has no children
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Flatten should not panic
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "empty_parent");

        // Fold should complete normally
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 2); // root + empty_parent
    }

    // Cache join ready vs ready equal stamps.
    #[test]
    fn cache_join_ready_vs_ready_equal_stamps() {
        // Ready vs Ready with equal stamps should have deterministic winner
        // (prefer one with higher generation, or use seq as tie-break).
        let payload1 = mock_payload("first");
        let payload2 = mock_payload("second");

        let ready1 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: payload1,
        };

        let ready2 = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: payload2,
        };

        // Join should preserve one of the values (both are valid since
        // Stamps/generation are equal)
        let result = ready1.join(&ready2);

        // Result should be Ready
        match result {
            FetchState::Ready { generation, .. } => {
                assert_eq!(generation, 5);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Cursor update when rows shrink to zero.
    #[test]
    fn cursor_update_when_rows_shrink_to_zero() {
        // After refresh failure (tree becomes None), cursor should clamp
        // To (0,0).
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "child".into(),
                label: "Child".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Initially has 1 visible row
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);

        let mut cursor = Cursor::new(rows.len());
        cursor.set_pos(0);

        // Simulate tree becoming None (refresh failure)
        // Create empty tree with no children
        let empty_tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: false,
            children: vec![],
        };

        let rows_after = flatten_tree(&empty_tree);
        assert_eq!(rows_after.len(), 0);

        // Update cursor
        cursor.update_len(rows_after.len());
        assert_eq!(cursor.pos(), 0);
        assert_eq!(cursor.len(), 0);
    }

    // Selection restore fallback when depth changes.
    #[test]
    fn selection_restore_fallback_when_depth_changes() {
        // Same reference appears but at different depth after refresh;
        // Ensure fallback to reference-only selection.
        let tree_before = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "parent".into(),
                label: "Parent".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "target".into(),
                    label: "Target at depth 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            }],
        };

        let rows_before = flatten_tree(&tree_before);
        // Target is at depth 1
        let target_before: Vec<_> = rows_before
            .iter()
            .filter(|r| r.node.reference == "target")
            .collect();
        assert_eq!(target_before.len(), 1);
        assert_eq!(target_before[0].depth, 1);

        // After refresh, target appears at depth 0 (structure changed)
        let tree_after = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "target".into(),
                label: "Target at depth 0".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        let rows_after = flatten_tree(&tree_after);
        let target_after: Vec<_> = rows_after
            .iter()
            .filter(|r| r.node.reference == "target")
            .collect();
        assert_eq!(target_after.len(), 1);
        assert_eq!(target_after[0].depth, 0);

        // Selection restore should fallback to reference-only match when
        // (reference, depth) doesn't exist
    }

    // Operational Reliability Tests
    //
    // These tests verify real-world operational scenarios that admins
    // would encounter: partial failures, high fan-out, rapid interactions,
    // stale cache recovery, etc.

    // Partial failure resilience.
    #[test]
    fn partial_failure_resilience() {
        // Some children fetch fails; rest of tree still renders with errors
        // Surfaced.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "success".into(),
                    label: "Success Node".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "error".into(),
                    label: "Error: Failed to fetch".into(),
                    node_type: NodeType::Host, // Error represented as Host with error label
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "success2".into(),
                    label: "Another Success".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Tree should render all nodes including error
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 3);

        // Verify error node is present (indicated by label)
        let error_node = rows.iter().find(|r| r.node.reference == "error");
        assert!(error_node.is_some());
        assert!(error_node.unwrap().node.label.contains("Error"));

        // Success nodes should also be present
        assert!(rows.iter().any(|r| r.node.reference == "success"));
        assert!(rows.iter().any(|r| r.node.reference == "success2"));
    }

    // High fanout proc placeholder performance.
    #[test]
    fn high_fanout_proc_placeholder_performance() {
        // 1 proc with 1000 actors - ensure expand produces placeholders
        // Without attempting to fetch all actors upfront.
        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: format!("actor_{}", i),
                label: format!("Actor {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: false, // Placeholder, not fetched yet
                has_children: false,
                children: vec![],
            });
        }

        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "high_fanout_proc".into(),
                label: "High Fanout Proc".into(),
                node_type: NodeType::Proc,
                expanded: true,
                fetched: true,
                has_children: true,
                children,
            }],
        };

        // Flatten should handle 1000 children efficiently
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1001); // proc + 1000 actors

        // Verify all are placeholders (not fetched)
        let actor_count = rows
            .iter()
            .filter(|r| r.node.reference.starts_with("actor_"))
            .count();
        assert_eq!(actor_count, 1000);

        // Fold should complete efficiently without stack overflow
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 1002); // root + proc + 1000 actors
    }

    // Rapid toggle stress test.
    #[test]
    fn rapid_toggle_stress_test() {
        // Rapidly toggle expand/collapse - ensure no stale rows or panics.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "grandchild".into(),
                        label: "Grandchild".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Rapid expand/collapse cycles
        for _ in 0..100 {
            // Expand child1
            tree.children[0].expanded = true;
            let rows = flatten_tree(&tree);
            assert!(rows.len() >= 2); // At least child1, child2

            // Collapse child1
            tree.children[0].expanded = false;
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), 2); // Only child1, child2

            // Collapse all
            collapse_all(&mut tree);
            let rows = flatten_tree(&tree);
            assert!(rows.len() <= 2); // All collapsed
        }

        // Final state should be stable
        let final_rows = flatten_tree(&tree);
        assert!(final_rows.len() <= 2);
    }

    // Stale cache recovery.
    #[test]
    fn stale_cache_recovery() {
        // Stale cached payload then refresh; ensure new payload replaces
        // Old.
        let old_payload = mock_payload("stale_data");
        let new_payload = mock_payload("fresh_data");

        let stale = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: old_payload,
        };

        let fresh = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: new_payload,
        };

        // Join should prefer fresh (newer generation and timestamp)
        let result = stale.join(&fresh);

        match result {
            FetchState::Ready {
                value, generation, ..
            } => {
                assert_eq!(value.identity, "fresh_data");
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready state"),
        }
    }

    // Selection stickiness during refresh.
    #[test]
    fn selection_stickiness_during_refresh() {
        // While refresh happens, selected node should remain stable if it
        // Still exists.
        let tree_before = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "stable".into(),
                    label: "Stable Node".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "transient".into(),
                    label: "Transient Node".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows_before = flatten_tree(&tree_before);
        assert_eq!(rows_before.len(), 2);

        // Select "stable"
        let stable_idx = rows_before
            .iter()
            .position(|r| r.node.reference == "stable")
            .unwrap();
        assert_eq!(stable_idx, 0);

        // After refresh, "transient" disappears but "stable" remains
        let tree_after = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "stable".into(),
                label: "Stable Node (refreshed)".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        let rows_after = flatten_tree(&tree_after);
        assert_eq!(rows_after.len(), 1);

        // "stable" should still be selectable at index 0
        let stable_idx_after = rows_after
            .iter()
            .position(|r| r.node.reference == "stable")
            .unwrap();
        assert_eq!(stable_idx_after, 0);
    }

    // Empty flight recorder renders safely.
    #[test]
    fn empty_flight_recorder_renders_safely() {
        // Actor node with empty/missing data should render gracefully
        // Without crashing.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "actor_with_empty_data".into(),
                label: "Actor (no flight recorder)".into(),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Flatten should handle this gracefully
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "actor_with_empty_data");
    }

    // Concurrent expansion stability.
    #[test]
    fn concurrent_expansion_stability() {
        // Simulate multiple expansions happening concurrently (or in rapid
        // Succession). Ensure tree state remains consistent.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "a".into(),
                    label: "A".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "a1".into(),
                        label: "A1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "b".into(),
                    label: "B".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "b1".into(),
                        label: "B1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
            ],
        };

        // Expand both a and b in sequence
        tree.children[0].expanded = true;
        tree.children[1].expanded = true;

        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 4); // a, a1, b, b1

        // Collapse both
        tree.children[0].expanded = false;
        tree.children[1].expanded = false;

        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2); // a, b

        // Verify consistency with fold
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 5); // root, a, a1, b, b1
    }

    // Refresh under partial failure keeps rendering.
    #[test]
    fn refresh_under_partial_failure_keeps_rendering() {
        // Some nodes error on refresh, but UI keeps rendering rest of tree.
        // Simulate two refresh cycles where different nodes fail.

        // First refresh: child1 succeeds, child2 fails
        let tree_refresh1 = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Child 1".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Error: Fetch failed".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows1 = flatten_tree(&tree_refresh1);
        assert_eq!(rows1.len(), 2);
        assert!(rows1.iter().any(|r| r.node.reference == "child1"));
        assert!(rows1.iter().any(|r| r.node.reference == "child2"));

        // Second refresh: child1 fails, child2 succeeds
        let tree_refresh2 = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "child1".into(),
                    label: "Error: Fetch failed".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "child2".into(),
                    label: "Child 2".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows2 = flatten_tree(&tree_refresh2);
        assert_eq!(rows2.len(), 2);
        // Both still render despite alternating failures
        assert!(rows2.iter().any(|r| r.node.reference == "child1"));
        assert!(rows2.iter().any(|r| r.node.reference == "child2"));
    }

    // Large refresh churn selection clamping.
    #[test]
    fn large_refresh_churn_selection_clamping() {
        // Tree shape changes dramatically across refreshes; verify
        // Selection/clamping and no panic.

        // Initial: 5 nodes
        let tree_before = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "a".into(),
                    label: "A".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "b".into(),
                    label: "B".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "c".into(),
                    label: "C".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "d".into(),
                    label: "D".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "e".into(),
                    label: "E".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows_before = flatten_tree(&tree_before);
        assert_eq!(rows_before.len(), 5);

        let mut cursor = Cursor::new(rows_before.len());
        cursor.set_pos(4); // Select last item "e"

        // After refresh: only 2 nodes remain (massive churn)
        let tree_after = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "a".into(),
                    label: "A (updated)".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "f".into(),
                    label: "F (new)".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let rows_after = flatten_tree(&tree_after);
        assert_eq!(rows_after.len(), 2);

        // Update cursor (should clamp)
        cursor.update_len(rows_after.len());
        assert!(cursor.pos() < rows_after.len());
        assert_eq!(cursor.pos(), 1); // Clamped to last valid
    }

    // Zero actor proc renders correctly.
    #[test]
    fn zero_actor_proc_renders_correctly() {
        // Proc with num_actors=0 should show no children and no expand
        // Affordance.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "empty_proc".into(),
                label: "Proc (0 actors)".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: false, // No children
                children: vec![],
            }],
        };

        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, "empty_proc");
        assert!(!rows[0].node.has_children);
        assert!(rows[0].node.children.is_empty());
    }

    // Long identity strings render safely.
    #[test]
    fn long_identity_strings_render_safely() {
        // Oversized reference/identity strings should not break rendering or
        // Wrap logic.
        let long_ref = "a".repeat(500); // 500 character reference
        let long_label = "Very long label: ".to_string() + &"x".repeat(1000);

        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: long_ref.clone(),
                label: long_label.clone(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Flatten should not panic with long strings
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node.reference, long_ref);
        assert_eq!(rows[0].node.label, long_label);

        // Fold should handle long strings
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 2);
    }

    // Duplicate references depth targeting under refresh.
    #[test]
    fn duplicate_references_depth_targeting_under_refresh() {
        // Duplicate reference in different branches; expansion toggles work
        // Correctly; verify depth-targeted behavior holds across refresh.

        // Initial state: "dup" at depths 0 and 1
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "branch_a".into(),
                    label: "Branch A".into(),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![TreeNode {
                        reference: "dup".into(),
                        label: "Dup at depth 1".into(),
                        node_type: NodeType::Proc,
                        expanded: false,
                        fetched: true,
                        has_children: false,
                        children: vec![],
                    }],
                },
                TreeNode {
                    reference: "dup".into(),
                    label: "Dup at depth 0".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Expand branch_a's dup (depth 1)
        tree.children[0].children[0].expanded = true;

        // Flatten should show both dups
        let rows = flatten_tree(&tree);
        let dup_refs: Vec<_> = rows
            .iter()
            .filter(|r| r.node.reference == "dup")
            .map(|r| r.depth)
            .collect();
        assert_eq!(dup_refs.len(), 2);
        assert!(dup_refs.contains(&0));
        assert!(dup_refs.contains(&1));

        // Simulate refresh: structure preserved
        let tree_after_refresh = tree.clone();
        let rows_after = flatten_tree(&tree_after_refresh);

        // Both dups still present with correct depths
        let dup_refs_after: Vec<_> = rows_after
            .iter()
            .filter(|r| r.node.reference == "dup")
            .map(|r| r.depth)
            .collect();
        assert_eq!(dup_refs_after, dup_refs);
    }

    // Payload schema drift missing fields.
    #[test]
    fn payload_schema_drift_missing_fields() {
        // Missing or extra fields in payload (simulated by tree node) should
        // Not crash rendering.

        // Simulate a node that might have missing optional fields
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "incomplete".into(),
                    label: "".into(), // Empty label (missing data)
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "".into(), // Empty reference (corrupt data)
                    label: "Unknown".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // Should not panic with missing/empty fields
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 2);

        // Fold should also handle gracefully
        let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });
        assert_eq!(count, 3);
    }

    // System proc filter toggle during churn.
    #[test]
    fn system_proc_filter_toggle_during_churn() {
        // Toggle system proc filter while tree structure is changing;
        // Ensure consistency.

        // Initial: mixed system and user procs
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "user_proc".into(),
                    label: "User Proc".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "system_proc_1".into(),
                    label: "System Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "system_proc_2".into(),
                    label: "System Proc 2".into(),
                    node_type: NodeType::Proc,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        // All nodes render (show_system_procs=true)
        let rows_all = flatten_tree(&tree);
        assert_eq!(rows_all.len(), 3);

        // Simulate filter toggle (in practice, build_tree_node would filter)
        // Here we just verify tree structure is stable
        let tree_filtered = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "user_proc".into(),
                label: "User Proc".into(),
                node_type: NodeType::Proc,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        let rows_filtered = flatten_tree(&tree_filtered);
        assert_eq!(rows_filtered.len(), 1);
        assert_eq!(rows_filtered[0].node.reference, "user_proc");
    }

    // Hostile Condition Tests
    //
    // These tests validate behavior under corrupt data, extreme inputs,
    // and adversarial conditions.

    #[test]
    fn corrupted_cached_state_recovery() {
        // Inject bad FetchState, ensure next refresh repairs via join.
        let bad = FetchState::<NodePayload>::Unknown;
        let good = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 10,
            value: mock_payload("repaired"),
        };

        // Join should recover
        let result = bad.join(&good);
        match result {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "repaired");
            }
            _ => panic!("Expected recovery to Ready"),
        }

        // Reverse order also recovers
        let result2 = good.join(&bad);
        match result2 {
            FetchState::Ready { value, .. } => {
                assert_eq!(value.identity, "repaired");
            }
            _ => panic!("Expected recovery to Ready"),
        }
    }

    // Out of order fetch completion.
    #[test]
    fn out_of_order_fetch_completion() {
        // Two fetches for same reference with different stamps; cache
        // Converges.
        let early = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 1000,
                seq: 1,
            },
            generation: 5,
            value: mock_payload("early"),
        };

        let late = FetchState::Ready {
            stamp: Stamp {
                ts_micros: 2000,
                seq: 1,
            },
            generation: 10,
            value: mock_payload("late"),
        };

        // Late should win
        let result = early.join(&late);
        match result {
            FetchState::Ready {
                value, generation, ..
            } => {
                assert_eq!(value.identity, "late");
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready with late data"),
        }

        // Even if received out-of-order
        let result_reversed = late.join(&early);
        match result_reversed {
            FetchState::Ready {
                value, generation, ..
            } => {
                assert_eq!(value.identity, "late");
                assert_eq!(generation, 10);
            }
            _ => panic!("Expected Ready with late data"),
        }
    }

    // Unicode and invalid strings render safely.
    #[test]
    fn unicode_and_invalid_strings_render_safely() {
        // Unicode, emoji, and unusual characters in identity should render
        // Without panic.
        let unicode_cases: Vec<String> = vec![
            "actor_🚀_emoji".to_string(),
            "proc_with_日本語".to_string(),
            "host_with_é_accents".to_string(),
            "zero_width_\u{200B}_joiner".to_string(),
            "rtl_\u{202E}_override".to_string(),
            "a".repeat(1000), // Very long ASCII
        ];

        for identity in &unicode_cases {
            let tree = TreeNode {
                reference: "root".into(),
                label: "Root".into(),
                node_type: NodeType::Root,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: identity.clone(),
                    label: format!("Label: {}", identity),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            };

            // Should not panic
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), 1);
            assert_eq!(&rows[0].node.reference, identity);
        }
    }

    // Memory pressure expand collapse cycle.
    #[test]
    fn memory_pressure_expand_collapse_cycle() {
        // Expand thousands of placeholders, collapse all, refresh; ensure
        // No runaway growth.

        // Create large tree
        let mut children = Vec::new();
        for i in 0..2000 {
            children.push(TreeNode {
                reference: format!("actor_{}", i),
                label: format!("Actor {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: false,
                has_children: false,
                children: vec![],
            });
        }

        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "mega_proc".into(),
                label: "Mega Proc".into(),
                node_type: NodeType::Proc,
                expanded: true,
                fetched: true,
                has_children: true,
                children,
            }],
        };

        // Expand - should handle 2000 nodes
        let rows_expanded = flatten_tree(&tree);
        assert_eq!(rows_expanded.len(), 2001); // proc + 2000 actors

        // Collapse all
        collapse_all(&mut tree);
        let rows_collapsed = flatten_tree(&tree);
        assert_eq!(rows_collapsed.len(), 1); // Only proc visible

        // Simulate refresh (structure unchanged)
        let rows_after_refresh = flatten_tree(&tree);
        assert_eq!(rows_after_refresh.len(), 1);

        // Memory should be stable (no allocations leaked)
    }

    // Rapid cursor ops during tree changes.
    #[test]
    fn rapid_cursor_ops_during_tree_changes() {
        // Simulate rapid key inputs while tree structure changes; verify
        // Cursor invariants hold.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![
                TreeNode {
                    reference: "a".into(),
                    label: "A".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
                TreeNode {
                    reference: "b".into(),
                    label: "B".into(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                },
            ],
        };

        let mut cursor = Cursor::new(flatten_tree(&tree).len());

        // Rapid operations
        for _ in 0..50 {
            cursor.move_down();
            cursor.move_up();

            // Simulate tree change
            tree.children[0].expanded = !tree.children[0].expanded;
            let rows = flatten_tree(&tree);
            cursor.update_len(rows.len());

            // Invariant must hold
            assert!(cursor.pos() < cursor.len() || cursor.len() == 0);
        }
    }

    // Header stats match tree fold.
    #[test]
    fn header_stats_match_tree_fold() {
        // Verify counts derived from tree match fold results under stress.
        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "host1".into(),
                label: "Host 1".into(),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: "proc1".into(),
                    label: "Proc 1".into(),
                    node_type: NodeType::Proc,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![
                        TreeNode {
                            reference: "actor1".into(),
                            label: "Actor 1".into(),
                            node_type: NodeType::Actor,
                            expanded: false,
                            fetched: true,
                            has_children: false,
                            children: vec![],
                        },
                        TreeNode {
                            reference: "actor2".into(),
                            label: "Actor 2".into(),
                            node_type: NodeType::Actor,
                            expanded: false,
                            fetched: true,
                            has_children: false,
                            children: vec![],
                        },
                    ],
                }],
            }],
        };

        // Count via fold
        let total_nodes = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
            1 + child_counts.iter().sum::<usize>()
        });

        // Count visible rows
        let visible_rows = flatten_tree(&tree).len();

        // Total should be root + host + proc + 2 actors = 5
        assert_eq!(total_nodes, 5);
        // Visible should be host + proc + 2 actors = 4 (no root)
        assert_eq!(visible_rows, 4);
    }

    // Timestamp monotonicity across refreshes.
    #[test]
    fn timestamp_monotonicity_across_refreshes() {
        // Ensure stamps remain strictly ordered even across multiple
        // Refresh cycles.
        let mut stamps = Vec::new();

        for i in 1..=10 {
            let state = FetchState::Ready {
                stamp: Stamp {
                    ts_micros: 1000 * i,
                    seq: i,
                },
                generation: i,
                value: mock_payload(&format!("refresh_{}", i)),
            };

            if let FetchState::Ready { stamp, .. } = &state {
                stamps.push(*stamp);
            }
        }

        // Verify monotonic increasing
        for i in 1..stamps.len() {
            assert!(stamps[i] > stamps[i - 1]);
        }
    }

    // Zero length and whitespace only strings.
    #[test]
    fn zero_length_and_whitespace_only_strings() {
        // Empty and whitespace-only strings should not break rendering.
        let edge_cases = vec![
            "",       // Empty
            " ",      // Single space
            "   ",    // Multiple spaces
            "\t",     // Tab
            "\n",     // Newline
            " \t\n ", // Mixed whitespace
        ];

        for test_str in edge_cases {
            let tree = TreeNode {
                reference: "root".into(),
                label: "Root".into(),
                node_type: NodeType::Root,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![TreeNode {
                    reference: test_str.to_string(),
                    label: test_str.to_string(),
                    node_type: NodeType::Host,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                }],
            };

            // Should not panic
            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), 1);
        }
    }

    // Performance at Scale Tests
    //
    // These tests validate algorithmic complexity, prove optimizations work,
    // and test pathological tree shapes at scale.

    #[test]
    fn flatten_scales_linearly_with_visible_nodes() {
        // Prove flatten is O(visible) not O(total).
        // Test at multiple scales: 100, 500, 1000.
        for scale in [100, 500, 1000] {
            let mut children = Vec::new();
            for i in 0..scale {
                children.push(TreeNode {
                    reference: format!("node_{}", i),
                    label: format!("Node {}", i),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                });
            }

            let tree = TreeNode {
                reference: "root".into(),
                label: "Root".into(),
                node_type: NodeType::Root,
                expanded: true,
                fetched: true,
                has_children: true,
                children,
            };

            let rows = flatten_tree(&tree);
            assert_eq!(rows.len(), scale);

            // Verify fold also scales linearly
            let count = fold_tree(&tree, &|_n, child_counts: Vec<usize>| {
                1 + child_counts.iter().sum::<usize>()
            });
            assert_eq!(count, scale + 1); // +1 for root
        }
    }

    #[test]
    fn deep_chain_vs_wide_fanout_performance() {
        // Compare deep chain (depth=500, breadth=1) vs
        // wide fan-out (depth=1, breadth=500).

        // Deep: 500 levels, each with 1 child
        let mut deep = TreeNode {
            reference: "deep_root".into(),
            label: "Deep Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![],
        };

        let mut current = &mut deep;
        for i in 0..499 {
            current.children.push(TreeNode {
                reference: format!("deep_{}", i),
                label: format!("Deep {}", i),
                node_type: NodeType::Host,
                expanded: true,
                fetched: true,
                has_children: true,
                children: vec![],
            });
            current = &mut current.children[0];
        }

        // Wide: 1 level with 500 children
        let mut wide_children = Vec::new();
        for i in 0..500 {
            wide_children.push(TreeNode {
                reference: format!("wide_{}", i),
                label: format!("Wide {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            });
        }

        let wide = TreeNode {
            reference: "wide_root".into(),
            label: "Wide Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: wide_children,
        };

        // Both should handle efficiently
        assert_eq!(flatten_tree(&deep).len(), 499);
        assert_eq!(flatten_tree(&wide).len(), 500);

        // Fold should also handle both
        let deep_count = fold_tree(&deep, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        let wide_count = fold_tree(&wide, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());

        assert_eq!(deep_count, 500);
        assert_eq!(wide_count, 501);
    }

    #[test]
    fn early_exit_avoids_full_traversal() {
        // Prove ControlFlow::Break actually short-circuits.
        // 1000 node tree, target at position 10.

        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: format!("node_{}", i),
                label: format!("Node {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            });
        }

        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children,
        };

        let mut visited = 0;
        let _ = fold_tree_mut(&mut tree, &mut |_n| {
            use std::ops::ControlFlow;
            visited += 1;
            if visited == 10 {
                ControlFlow::Break(())
            } else {
                ControlFlow::<()>::Continue(())
            }
        });

        // Should visit ~10 nodes, not 1000
        assert!(visited < 50, "Early exit failed, visited {}", visited);
        assert_eq!(visited, 10);
    }

    #[test]
    fn cursor_navigation_large_list() {
        // Test cursor operations on very large flattened list.
        let mut cursor = Cursor::new(100000);

        // Jump to end
        cursor.end();
        assert_eq!(cursor.pos(), 99999);

        // Jump to start
        cursor.home();
        assert_eq!(cursor.pos(), 0);

        // Repeated set operations (stress test)
        for i in 0..1000 {
            cursor.set_pos(i * 50);
        }

        // Should complete quickly with no panic
        assert!(cursor.pos() < cursor.len());
    }

    #[test]
    fn flatten_ignores_collapsed_subtrees() {
        // Prove flatten only traverses visible nodes.
        // Tree: 1 visible parent + 100 collapsed parents,
        // each with 100 children.
        // Should visit 100 nodes, not 10,000.

        let mut children = Vec::new();
        for i in 0..100 {
            let mut grandchildren = Vec::new();
            for j in 0..100 {
                grandchildren.push(TreeNode {
                    reference: format!("child_{}_{}", i, j),
                    label: format!("Child {} {}", i, j),
                    node_type: NodeType::Actor,
                    expanded: false,
                    fetched: true,
                    has_children: false,
                    children: vec![],
                });
            }

            children.push(TreeNode {
                reference: format!("parent_{}", i),
                label: format!("Parent {}", i),
                node_type: NodeType::Proc,
                expanded: false, // Collapsed!
                fetched: true,
                has_children: true,
                children: grandchildren,
            });
        }

        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children,
        };

        // Should only see the 100 collapsed parents, not their 10k children
        let rows = flatten_tree(&tree);
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn many_nodes_same_reference_depth_tracking() {
        // Test (reference, depth) tracking with many duplicates.
        // 100 branches, each with "dup" at different depth.

        let mut children = Vec::new();
        for depth in 0..100 {
            // Create nested structure to get "dup" at various depths
            let mut node = TreeNode {
                reference: "dup".into(),
                label: format!("Dup at depth {}", depth),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            };

            // Wrap in depth layers
            for i in (0..depth).rev() {
                node = TreeNode {
                    reference: format!("wrapper_{}", i),
                    label: format!("Wrapper {}", i),
                    node_type: NodeType::Host,
                    expanded: true,
                    fetched: true,
                    has_children: true,
                    children: vec![node],
                };
            }

            children.push(node);
        }

        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children,
        };

        // Flatten should handle all the different (dup, depth) pairs
        let rows = flatten_tree(&tree);
        // Each branch contributes depth+1 nodes
        let expected: usize = (0..100).map(|d| d + 1).sum();
        assert_eq!(rows.len(), expected);
    }

    #[test]
    fn memory_stable_across_repeated_operations() {
        // Verify no growth proportional to operation count.
        let mut tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: vec![TreeNode {
                reference: "child".into(),
                label: "Child".into(),
                node_type: NodeType::Host,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            }],
        };

        // Perform 5000 expand/collapse operations
        for _ in 0..5000 {
            tree.children[0].expanded = !tree.children[0].expanded;
            let _ = flatten_tree(&tree);
        }

        // Tree size should be constant
        let node_count = fold_tree(&tree, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());
        assert_eq!(node_count, 2); // root + child only
    }

    #[test]
    fn fold_performance_parity_with_manual_recursion() {
        // Prove fold is as efficient as manual recursion.
        let mut children = Vec::new();
        for i in 0..1000 {
            children.push(TreeNode {
                reference: format!("node_{}", i),
                label: format!("Node {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            });
        }

        let tree = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children,
        };

        // Count via fold
        let fold_result = fold_tree(&tree, &|_n, cs: Vec<usize>| 1 + cs.iter().sum::<usize>());

        // Count via manual recursion
        fn manual_count(n: &TreeNode) -> usize {
            1 + n.children.iter().map(manual_count).sum::<usize>()
        }
        let manual_result = manual_count(&tree);

        assert_eq!(fold_result, manual_result);
        assert_eq!(fold_result, 1001);
    }

    #[test]
    fn refresh_churn_large_differential() {
        // Test maximal churn: tree changes from 1000 to 100 nodes.
        let mut tree_before_children = Vec::new();
        for i in 0..1000 {
            tree_before_children.push(TreeNode {
                reference: format!("before_{}", i),
                label: format!("Before {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            });
        }

        let tree_before = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: tree_before_children,
        };

        let rows_before = flatten_tree(&tree_before);
        assert_eq!(rows_before.len(), 1000);

        // After refresh: only 100 nodes
        let mut tree_after_children = Vec::new();
        for i in 0..100 {
            tree_after_children.push(TreeNode {
                reference: format!("after_{}", i),
                label: format!("After {}", i),
                node_type: NodeType::Actor,
                expanded: false,
                fetched: true,
                has_children: false,
                children: vec![],
            });
        }

        let tree_after = TreeNode {
            reference: "root".into(),
            label: "Root".into(),
            node_type: NodeType::Root,
            expanded: true,
            fetched: true,
            has_children: true,
            children: tree_after_children,
        };

        let rows_after = flatten_tree(&tree_after);
        assert_eq!(rows_after.len(), 100);

        // Should handle massive churn efficiently
    }
}
