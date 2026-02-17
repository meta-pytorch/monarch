/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Interactive TUI client for the Monarch mesh admin HTTP API.
//!
//! Displays the full topology as a navigable tree: host → proc → actor.
//! Selecting any node shows contextual details on the right pane,
//! including actor flight recorder events when an actor is selected.
//!
//! ```bash
//! # Terminal 1: Run dining philosophers (or any hyperactor application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_example_dining_philosophers -- --in-process
//!
//! # Terminal 2: Run this TUI (use the port printed by the application)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_admin_tui -- --addr 127.0.0.1:XXXXX
//! ```

use std::collections::HashSet;
use std::io;
use std::io::IsTerminal;
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
use hyperactor::admin::ActorDetails;
use hyperactor::admin::HostDetails;
use hyperactor::admin::HostProcEntry;
use hyperactor::admin::HostSummary;
use hyperactor::admin::ProcDetails;
use hyperactor::clock::Clock;
use hyperactor::clock::RealClock;
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
use serde::Deserialize;
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

// Client-side projection of the mesh admin `/v1/hosts` response.
// This type is private in mesh_admin.rs so we replicate it here for deserialization.
#[derive(Debug, Deserialize)]
struct MeshHostsResponse {
    #[allow(dead_code)]
    total: usize,
    hosts: Vec<HostSummary>,
    unreachable: Vec<String>,
}

// Topology tree model

/// What kind of entity a tree node represents.
#[derive(Debug, Clone)]
enum NodeKind {
    Host {
        addr: String,
    },
    Proc {
        host_addr: String,
        proc_name: String,
    },
    Actor {
        host_addr: String,
        proc_name: String,
        actor_name: String,
        /// Display label including pid (e.g. "sieve[0]"), used for
        /// unique expand keys when multiple actors share the same name.
        actor_label: String,
    },
}

impl NodeKind {
    /// Return a stable key for preserving state (expanded + selection) across refreshes.
    fn expand_key(&self) -> String {
        match self {
            NodeKind::Host { addr } => format!("host:{}", addr),
            NodeKind::Proc {
                host_addr,
                proc_name,
            } => format!("proc:{}:{}", host_addr, proc_name),
            NodeKind::Actor {
                host_addr,
                proc_name,
                actor_label,
                ..
            } => format!("actor:{}:{}:{}", host_addr, proc_name, actor_label),
        }
    }
}

/// A single node in the topology tree.
#[derive(Debug, Clone)]
struct TreeNode {
    label: String,
    depth: usize,
    kind: NodeKind,
    expanded: bool,
    has_children: bool,
}

/// Contextual detail for the currently selected node.
#[derive(Debug)]
enum NodeDetail {
    Host(HostDetails),
    Proc(ProcDetails),
    Actor(ActorDetails),
}

// Application state

struct App {
    base_url: String,
    client: reqwest::Client,
    should_quit: bool,

    /// Flattened topology tree: host → proc → actor.
    tree: Vec<TreeNode>,
    /// Currently selected index into visible_indices().
    selected: usize,
    /// Detail for the selected node (fetched on selection change).
    detail: Option<NodeDetail>,
    detail_error: Option<String>,

    last_refresh: String,
    error: Option<String>,

    /// Whether to show system/infrastructure procs (prefixed with `[system]`).
    show_system_procs: bool,
}

/// Result of handling a key event.
enum KeyResult {
    /// Nothing changed.
    None,
    /// Selection or expand/collapse changed; re-fetch detail.
    DetailChanged,
    /// A filter/view setting changed; full tree refresh needed.
    NeedsRefresh,
}

impl App {
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
        }
    }

    /// Compute which tree indices are visible (not hidden by collapsed ancestors).
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

    /// Get the tree index for the currently selected visible row.
    fn selected_tree_index(&self) -> Option<usize> {
        let visible = self.visible_indices();
        visible.get(self.selected).copied()
    }

    /// Rebuild the full topology tree from the admin API.
    ///
    /// 1. GET /v1/hosts → list of hosts
    /// 2. For each host, GET /v1/hosts/{addr} → proc names
    /// 3. For each proc, GET /v1/hosts/{addr}/procs/{name} → root actors
    async fn refresh(&mut self) {
        self.error = None;

        // Save expanded state before rebuilding
        let expanded_keys: HashSet<String> = self
            .tree
            .iter()
            .filter(|n| n.expanded)
            .map(|n| n.kind.expand_key())
            .collect();

        // Save current selection's expand_key to restore position
        let selected_key = self
            .selected_tree_index()
            .and_then(|idx| self.tree.get(idx))
            .map(|n| n.kind.expand_key());

        let (hosts, unreachable_hosts) = match self
            .client
            .get(format!("{}/v1/hosts", self.base_url))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<MeshHostsResponse>().await {
                    Ok(mesh_resp) => (mesh_resp.hosts, mesh_resp.unreachable),
                    Err(e) => {
                        self.error = Some(format!("Parse error: {}", e));
                        return;
                    }
                }
            }
            Ok(resp) => {
                self.error = Some(format!("HTTP {}", resp.status()));
                return;
            }
            Err(e) => {
                self.error = Some(format!("Failed to connect: {}", e));
                return;
            }
        };

        let mut tree = Vec::new();

        for host in &hosts {
            let host_key = format!("host:{}", host.addr);
            tree.push(TreeNode {
                label: host.addr.clone(),
                depth: 0,
                kind: NodeKind::Host {
                    addr: host.addr.clone(),
                },
                expanded: expanded_keys.contains(&host_key),
                has_children: true,
            });

            // Fetch host details to get proc names
            let host_details = self.fetch_host_details(&host.addr).await;
            let mut proc_entries: Vec<&HostProcEntry> = match &host_details {
                Some(hd) => hd
                    .procs
                    .iter()
                    .filter(|p| self.show_system_procs || !p.name.starts_with("[system]"))
                    .collect(),
                None => continue,
            };
            proc_entries.sort_by(|a, b| a.name.cmp(&b.name));

            // Update the host label to include proc count
            if let Some(host_node) = tree.last_mut() {
                host_node.label = format!("{}  ({} procs)", host.addr, proc_entries.len());
            }

            for entry in &proc_entries {
                self.append_proc_to_tree(&mut tree, &entry.name, &host.addr, 1, &expanded_keys)
                    .await;
            }
        }

        // Show unreachable hosts (greyed out, no children to expand)
        for addr in &unreachable_hosts {
            tree.push(TreeNode {
                label: format!("{} (unreachable)", addr),
                depth: 0,
                kind: NodeKind::Host { addr: addr.clone() },
                expanded: false,
                has_children: false,
            });
        }

        self.tree = tree;
        self.last_refresh = chrono_now();

        // Restore selection position
        let visible = self.visible_indices();
        if let Some(ref key) = selected_key {
            if let Some(pos) = visible.iter().position(|&idx| {
                self.tree
                    .get(idx)
                    .map(|n| n.kind.expand_key() == *key)
                    .unwrap_or(false)
            }) {
                self.selected = pos;
            }
        }
        // Clamp selection
        if !visible.is_empty() && self.selected >= visible.len() {
            self.selected = visible.len() - 1;
        }

        // Fetch detail for current selection
        self.fetch_selected_detail().await;
    }

    async fn fetch_host_details(&self, addr: &str) -> Option<HostDetails> {
        let url = format!("{}/v1/hosts/{}", self.base_url, url_encode(addr));
        let resp = self.client.get(&url).send().await.ok()?;
        if resp.status().is_success() {
            resp.json::<HostDetails>().await.ok()
        } else {
            None
        }
    }

    async fn fetch_proc_details(
        &self,
        host_addr: &str,
        proc_name: &str,
    ) -> Result<ProcDetails, String> {
        let url = format!(
            "{}/v1/hosts/{}/procs/{}",
            self.base_url,
            url_encode(host_addr),
            url_encode(proc_name),
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;
        if resp.status().is_success() {
            resp.json::<ProcDetails>()
                .await
                .map_err(|e| format!("Parse error: {}", e))
        } else {
            Err(format!("HTTP {}", resp.status()))
        }
    }

    /// Append a proc and its actors to the tree at the given depth.
    async fn append_proc_to_tree(
        &self,
        tree: &mut Vec<TreeNode>,
        proc_name: &str,
        host_addr: &str,
        depth: usize,
        expanded_keys: &HashSet<String>,
    ) {
        let kind = NodeKind::Proc {
            host_addr: host_addr.to_string(),
            proc_name: proc_name.to_string(),
        };
        let proc_key = kind.expand_key();
        let short_name = extract_short_proc_name(proc_name);
        tree.push(TreeNode {
            label: short_name,
            depth,
            kind,
            expanded: expanded_keys.contains(&proc_key),
            has_children: true,
        });

        // Fetch proc details to get actors
        if let Ok(pd) = self.fetch_proc_details(host_addr, proc_name).await {
            for root_actor in &pd.actors {
                // Parse the ActorId to get a short display label and the
                // plain actor name (used for URL construction — the handler
                // matches against `actor_id.name()`).
                let (label, actor_name) = match ActorId::from_str(root_actor) {
                    Ok(actor_id) => (
                        format!("{}[{}]", actor_id.name(), actor_id.pid()),
                        actor_id.name().to_string(),
                    ),
                    Err(_) => (root_actor.clone(), root_actor.clone()),
                };
                tree.push(TreeNode {
                    label: label.clone(),
                    depth: depth + 1,
                    kind: NodeKind::Actor {
                        host_addr: host_addr.to_string(),
                        proc_name: proc_name.to_string(),
                        actor_name,
                        actor_label: label,
                    },
                    expanded: false,
                    has_children: false,
                });
            }
        }
    }

    /// Fetch contextual detail for the currently selected tree node.
    async fn fetch_selected_detail(&mut self) {
        self.detail = None;
        self.detail_error = None;

        let tree_idx = match self.selected_tree_index() {
            Some(idx) => idx,
            None => return,
        };
        let node = match self.tree.get(tree_idx) {
            Some(n) => n.clone(),
            None => return,
        };

        match &node.kind {
            NodeKind::Host { addr } => match self.fetch_host_details(addr).await {
                Some(hd) => self.detail = Some(NodeDetail::Host(hd)),
                None => self.detail_error = Some("Failed to fetch host details".to_string()),
            },
            NodeKind::Proc {
                host_addr,
                proc_name,
            } => match self.fetch_proc_details(host_addr, proc_name).await {
                Ok(pd) => self.detail = Some(NodeDetail::Proc(pd)),
                Err(e) => self.detail_error = Some(e),
            },
            NodeKind::Actor {
                host_addr,
                proc_name,
                actor_name,
                ..
            } => {
                let url = format!(
                    "{}/v1/hosts/{}/procs/{}/{}",
                    self.base_url,
                    url_encode(host_addr),
                    url_encode(proc_name),
                    url_encode(actor_name),
                );
                match self.client.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        match resp.json::<ActorDetails>().await {
                            Ok(ad) => self.detail = Some(NodeDetail::Actor(ad)),
                            Err(_) => {
                                self.detail_error =
                                    Some("Failed to parse actor details".to_string());
                            }
                        }
                    }
                    _ => {
                        self.detail_error = Some("Failed to fetch actor details".to_string());
                    }
                }
            }
        }
    }

    /// Handle a key event.
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
                // Expand selected node
                if let Some(&tree_idx) = visible.get(self.selected) {
                    if let Some(node) = self.tree.get_mut(tree_idx) {
                        if node.has_children && !node.expanded {
                            node.expanded = true;
                            return KeyResult::DetailChanged;
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
                // Expand all
                for node in &mut self.tree {
                    if node.has_children {
                        node.expanded = true;
                    }
                }
                KeyResult::DetailChanged
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

/// URL-encode a string for use in URL path segments.
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

/// Extract a short display name from a full ProcId string.
///
/// ProcId::Direct format is `channel_addr,name`. We want just the `name` part.
/// System procs are prefixed with `[system] ` — preserve the prefix and shorten
/// the ProcId portion.
/// If there's no comma, return the whole string.
fn extract_short_proc_name(proc_id: &str) -> String {
    if let Some(inner) = proc_id.strip_prefix("[system] ") {
        let short = if let Some(pos) = inner.rfind(',') {
            &inner[pos + 1..]
        } else {
            inner
        };
        return format!("[system] {}", short);
    }
    if let Some(pos) = proc_id.rfind(',') {
        proc_id[pos + 1..].to_string()
    } else {
        proc_id.to_string()
    }
}

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

#[allow(clippy::disallowed_methods)]
fn chrono_now() -> String {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", hours, mins, s)
}

// Terminal setup / teardown

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

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
                                app.fetch_selected_detail().await;
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

fn render_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    render_topology_tree(frame, chunks[0], app);
    render_detail_pane(frame, chunks[1], app);
}

/// Render the topology tree: host → proc → actor.
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
                match node.kind {
                    NodeKind::Host { .. } => Style::default().fg(Color::Cyan),
                    NodeKind::Proc { .. } => Style::default().fg(Color::Green),
                    NodeKind::Actor { .. } => Style::default().fg(Color::White),
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

/// Render contextual details for the selected node.
fn render_detail_pane(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    match &app.detail {
        Some(NodeDetail::Actor(details)) => render_actor_detail(frame, area, details),
        Some(NodeDetail::Host(details)) => render_host_detail(frame, area, details),
        Some(NodeDetail::Proc(details)) => render_proc_detail(frame, area, details),
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

fn render_host_detail(frame: &mut ratatui::Frame<'_>, area: Rect, details: &HostDetails) {
    let block = Block::default()
        .title("Host Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Address: ", Style::default().fg(Color::Gray)),
            Span::raw(&details.addr),
        ]),
        Line::from(vec![
            Span::styled("Procs: ", Style::default().fg(Color::Gray)),
            Span::raw(details.procs.len().to_string()),
        ]),
    ];
    if let Some(url) = &details.agent_url {
        lines.push(Line::from(vec![
            Span::styled("Agent: ", Style::default().fg(Color::Gray)),
            Span::styled(url, Style::default().fg(Color::Blue)),
        ]));
    }
    lines.push(Line::default());
    for proc in &details.procs {
        let actor_info = if proc.num_actors > 0 {
            format!(" ({} actors)", proc.num_actors)
        } else {
            String::new()
        };
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(&proc.name, Style::default().fg(Color::Green)),
            Span::styled(actor_info, Style::default().fg(Color::DarkGray)),
        ]));
    }

    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

fn render_proc_detail(frame: &mut ratatui::Frame<'_>, area: Rect, details: &ProcDetails) {
    let block = Block::default()
        .title("Proc Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Name: ", Style::default().fg(Color::Gray)),
            Span::raw(&details.proc_name),
        ]),
        Line::from(vec![
            Span::styled("Actors: ", Style::default().fg(Color::Gray)),
            Span::raw(details.actors.len().to_string()),
        ]),
        Line::default(),
    ];
    for (i, actor) in details.actors.iter().enumerate() {
        if i >= 50 {
            lines.push(Line::from(Span::styled(
                format!("  ... and {} more", details.actors.len() - 50),
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

fn render_actor_detail(frame: &mut ratatui::Frame<'_>, area: Rect, details: &ActorDetails) {
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
                &details.actor_status,
                Style::default().fg(if details.actor_status == "Running" {
                    Color::Green
                } else {
                    Color::Yellow
                }),
            ),
        ]),
        Line::from(vec![
            Span::styled("Type: ", Style::default().fg(Color::Gray)),
            Span::raw(&details.actor_type),
        ]),
        Line::from(vec![
            Span::styled("Messages: ", Style::default().fg(Color::Gray)),
            Span::raw(details.messages_processed.to_string()),
        ]),
        Line::from(vec![
            Span::styled("Processing time: ", Style::default().fg(Color::Gray)),
            Span::raw(
                humantime::format_duration(std::time::Duration::from_micros(
                    details.total_processing_time_us,
                ))
                .to_string(),
            ),
        ]),
        Line::from(vec![
            Span::styled("Created: ", Style::default().fg(Color::Gray)),
            Span::raw(&details.created_at),
        ]),
        Line::from(vec![
            Span::styled("Last handler: ", Style::default().fg(Color::Gray)),
            Span::raw(details.last_message_handler.as_deref().unwrap_or("-")),
        ]),
        Line::from(vec![
            Span::styled("Children: ", Style::default().fg(Color::Gray)),
            Span::raw(details.children.len().to_string()),
        ]),
    ])
    .block(info_block);
    frame.render_widget(info, chunks[0]);

    // Flight recorder
    let recorder_block = Block::default()
        .title("Flight Recorder")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let events: Vec<Line> = if details.flight_recorder.is_empty() {
        vec![Line::from(Span::styled(
            "No events",
            Style::default().fg(Color::Gray),
        ))]
    } else {
        details
            .flight_recorder
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
                        format!(
                            "{} ",
                            event.timestamp.get(11..19).unwrap_or(&event.timestamp)
                        ),
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

fn render_footer(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let help = "q: quit | j/k: navigate | g/G: top/bottom | Tab/Shift-Tab: expand/collapse | e/c: expand/collapse all | s: system procs";
    let footer = Paragraph::new(help)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, area);
}
