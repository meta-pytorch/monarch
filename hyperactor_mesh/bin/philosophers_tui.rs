/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Interactive TUI client for the hyperactor admin HTTP API.
//!
//! Run alongside the dining_philosophers example to monitor actors in
//! real-time.
//!
//! ```bash
//! # Terminal 1: Run dining philosophers
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_example_dining_philosophers -- --in-process
//!
//! # Terminal 2: Run this TUI (use the port printed by
//! dining_philosophers)
//! buck2 run fbcode//monarch/hyperactor_mesh:hyperactor_mesh_example_philosophers_tui -- --addr 127.0.0.1:XXXXX
//! ```

use std::io;
use std::io::IsTerminal;
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
use ratatui::widgets::Paragraph;
use ratatui::widgets::Wrap;
use serde::Deserialize;
use serde_json::Value;

/// Command-line arguments for the `philosophers-tui` demo.
///
/// This program is a thin terminal UI client for the hyperactor admin
/// HTTP API. It connects to a running admin server (started by the
/// dining_philosophers example) and periodically refreshes the
/// displayed topology/details.
///
/// The intent is to demonstrate an *external* tool layered over the
/// admin API, not to reach into hyperactor internals.
#[derive(Debug, Parser)]
#[command(
    name = "philosophers-tui",
    about = "TUI client for hyperactor admin API"
)]
struct Args {
    /// Admin server address (e.g., 127.0.0.1:8080)
    #[arg(long, short)]
    addr: String,

    /// Refresh interval in milliseconds
    #[arg(long, default_value_t = 1000)]
    refresh_ms: u64,
}

/// Detailed runtime information for a single actor, as returned by
/// the admin API’s actor/reference endpoints (e.g. `GET
/// /{actor_ref}`).
///
/// This struct is intentionally a *projection* of the full
/// `ActorDetails` payload: it includes only the fields the TUI
/// actually renders. Any additional fields present in the server
/// response are safely ignored by serde.
#[derive(Debug, Clone, Deserialize)]
struct ActorDetails {
    /// Current lifecycle/status of the actor (e.g. "idle",
    /// "processing", "saving").
    actor_status: String,

    /// Concrete Rust type name of the actor instance.
    ///
    /// Useful for understanding what logic is running at a node
    /// during traversal.
    actor_type: String,

    /// Actor IDs for this actor’s immediate children in the
    /// supervision tree.
    ///
    /// The TUI can use these references to expand/collapse and/or
    /// recursively fetch details for descendants.
    children: Vec<String>,

    /// Recent tracing events from the actor-local flight recorder.
    ///
    /// This is a bounded ring buffer of structured events that
    /// provides quick “what just happened” context without needing
    /// external log aggregation.
    flight_recorder: Vec<RecordedEvent>,

    /// Total number of messages processed by this actor so far.
    messages_processed: u64,

    /// ISO 8601 timestamp indicating when the actor instance was
    /// created.
    created_at: String,

    /// Name of the last message handler invoked (typically derived
    /// from `HandlerInfo`).
    ///
    /// Useful for answering "what was this actor doing most
    /// recently?"
    last_message_handler: Option<String>,

    /// Cumulative wall-clock time spent inside message handlers, in
    /// microseconds.
    ///
    /// Useful as a coarse measure of "where time is going" per actor.
    total_processing_time_us: u64,
}

/// A single event from an actor's flight recorder.
///
/// Represents a structured tracing event emitted while the actor was
/// running. These are typically captured via `tracing`
/// instrumentation and stored in a bounded ring buffer local to the
/// actor.
#[derive(Debug, Clone, Deserialize)]
struct RecordedEvent {
    /// ISO 8601 timestamp indicating when the event was recorded.
    timestamp: String,

    /// Log level of the event (e.g. "INFO", "DEBUG", "WARN", "ERROR",
    /// "TRACE").
    level: String,

    /// Name of the tracing span or event.
    ///
    /// This usually corresponds to the handler, operation, or logical
    /// action being executed at the time of the event.
    name: String,

    /// Structured fields captured with the event.
    ///
    /// Contains key-value pairs from tracing instrumentation.
    #[serde(default)]
    fields: serde_json::Value,
}

/// A single node in the rendered tree view.
///
/// This is a UI-level representation derived from the admin API
/// responses, used to drive layout and navigation in the TUI.
#[derive(Debug, Clone)]
struct TreeNode {
    /// Human-readable label displayed in the tree (e.g. proc or actor
    /// ID).
    label: String,

    /// Depth in the tree hierarchy, used for indentation and layout.
    depth: usize,

    /// Proc identifier if this node represents a proc root.
    ///
    /// Exactly one of `proc_name` or `actor_name` is expected to be
    /// `Some`.
    proc_name: Option<String>,

    /// Actor identifier if this node represents an actor instance.
    actor_name: Option<String>,
}

/// Top-level TUI application state.
///
/// Owns the HTTP client and all UI model/state needed to render and
/// interact with the admin-backed tree and detail panes.
struct App {
    /// Base URL of the admin server (e.g. `http://127.0.0.1:38585`).
    base_url: String,

    /// Reused HTTP client for talking to the admin API.
    client: reqwest::Client,

    /// Set to true to exit the main event loop cleanly.
    should_quit: bool,

    // --
    // Tree view state
    /// Flattened pre-order list of nodes currently displayed in the
    /// tree pane.
    ///
    /// Each entry includes a depth for indentation and an optional
    /// proc/actor id for navigation/fetching details.
    tree_nodes: Vec<TreeNode>,

    /// Index into `tree_nodes` for the currently selected row.
    selected: usize,

    // --
    // Selected actor details
    /// Details for the currently selected actor (if the selection is
    /// an actor and the fetch succeeded).
    actor_details: Option<ActorDetails>,

    /// Error message from the most recent actor-details fetch, if
    /// any.
    detail_error: Option<String>,

    // --
    // Status
    /// Human-readable timestamp of the last successful refresh.
    last_refresh: String,

    /// Error message from the most recent refresh cycle (tree/proc
    /// fetch), if any.
    error: Option<String>,
}

impl App {
    /// Construct a new application instance targeting the given admin
    /// server address.
    ///
    /// `addr` should be a host:port pair (e.g. `"127.0.0.1:8080"`).
    /// We derive a base URL from it and initialize the rest of the UI
    /// state to empty/defaults.
    fn new(addr: &str) -> Self {
        Self {
            base_url: format!("http://{}", addr),
            client: reqwest::Client::new(),
            should_quit: false,
            tree_nodes: Vec::new(),
            selected: 0,
            actor_details: None,
            detail_error: None,
            last_refresh: String::new(),
            error: None,
        }
    }

    /// Refresh all UI state from the admin server.
    ///
    /// This performs two network reads:
    /// 1) Fetch `/tree` and rebuild the flattened `tree_nodes` list.
    /// 2) Fetch details for the currently selected node (if it
    ///    identifies an actor).
    ///
    /// Any errors are captured in `self.error`/`self.detail_error` so
    /// the UI can render them without panicking. On a successful tree
    /// fetch we also update `last_refresh`.
    async fn refresh(&mut self) {
        self.error = None;

        // Fetch tree view
        match self
            .client
            .get(format!("{}/tree", self.base_url))
            .send()
            .await
        {
            Ok(resp) => match resp.text().await {
                Ok(text) => {
                    self.parse_tree(&text);
                    self.last_refresh = chrono_now();
                }
                Err(e) => self.error = Some(format!("Failed to read response: {}", e)),
            },
            Err(e) => self.error = Some(format!("Failed to connect: {}", e)),
        }

        // Fetch details for selected node
        self.fetch_selected_details().await;
    }

    /// Parse the ASCII tree dump returned by `GET /tree` into
    /// `TreeNode`s.
    ///
    /// The admin endpoint returns one line per actor, where
    /// tree-drawing characters (`│`, `├`, `└`, `─`) and indentation
    /// encode depth. We flatten this into a `Vec<TreeNode>` that the
    /// TUI can treat as a simple scrollable list.
    ///
    /// Notes:
    /// - Empty lines are ignored.
    /// - Depth is inferred from the prefix before the actor label.
    /// - The actor label portion is later decoded into `proc_name` /
    ///   `actor_name` (depending on the exact string format used by
    ///   the admin API).
    fn parse_tree(&mut self, text: &str) {
        self.tree_nodes.clear();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }

            // Count leading spaces/tree chars to determine depth
            let trimmed = line.trim_start_matches([' ', '│', '├', '└', '─']);
            let depth = if line.starts_with(|c: char| c.is_alphanumeric() || c == '[') {
                0
            } else {
                (line.len() - trimmed.len()) / 4
            };

            // Extract the actor ID (before any " -> " URL suffix)
            let label = trimmed
                .split("  ->  ")
                .next()
                .unwrap_or(trimmed)
                .to_string();

            // Parse proc and actor names from the ID
            let (proc_name, actor_name) = parse_actor_id(&label);

            self.tree_nodes.push(TreeNode {
                label,
                depth,
                proc_name,
                actor_name,
            });
        }

        // Ensure selection is valid
        if self.selected >= self.tree_nodes.len() && !self.tree_nodes.is_empty() {
            self.selected = self.tree_nodes.len() - 1;
        }
    }

    /// Fetch and cache details for the currently selected tree node.
    ///
    /// This is called after each tree refresh (and typically after
    /// selection changes). If the selected node does not correspond
    /// to an actor reference, we leave `actor_details` as `None`. Any
    /// fetch/parse failures are recorded in `detail_error` for
    /// display in the details pane.
    ///
    /// The function resets `actor_details` up front so the UI never
    /// shows stale information for a new selection.
    async fn fetch_selected_details(&mut self) {
        self.actor_details = None;
        self.detail_error = None;

        if let Some(node) = self.tree_nodes.get(self.selected) {
            if let (Some(proc), Some(actor)) = (&node.proc_name, &node.actor_name) {
                let url = format!("{}/procs/{}/{}", self.base_url, proc, actor);
                match self.client.get(&url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            match resp.json::<ActorDetails>().await {
                                Ok(details) => self.actor_details = Some(details),
                                Err(e) => self.detail_error = Some(format!("Parse error: {}", e)),
                            }
                        } else {
                            self.detail_error = Some(format!("HTTP {}", resp.status()));
                        }
                    }
                    Err(e) => self.detail_error = Some(format!("Request failed: {}", e)),
                }
            }
        }
    }

    /// Handle a single keyboard event and update application state.
    ///
    /// This is the main input dispatcher for the TUI. It interprets
    /// key presses (navigation, selection changes, refresh/quit,
    /// etc.) and mutates the in-memory UI state accordingly (e.g.
    /// `selected`, `should_quit`).
    ///
    /// The actual effect of each key is encoded in the `match` below
    /// so the key bindings are easy to scan and tweak.
    fn on_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if self.selected > 0 {
                    self.selected -= 1;
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if self.selected + 1 < self.tree_nodes.len() {
                    self.selected += 1;
                }
            }
            KeyCode::Home => self.selected = 0,
            KeyCode::End => {
                if !self.tree_nodes.is_empty() {
                    self.selected = self.tree_nodes.len() - 1;
                }
            }
            _ => {}
        }
    }
}

/// Parse an actor ID into (proc_id_string, actor_name).
///
/// ActorId formats (from hyperactor::reference):
/// - Ranked: `world[rank].actor[pid]` -> proc=`world[rank]`, actor=`actor`
/// - Direct: `channel_addr,proc_name,actor[pid]` -> proc=`channel_addr,proc_name`, actor=`actor`
///
/// The actor_name should NOT include the `[pid]` suffix for the API
/// call.
fn parse_actor_id(id: &str) -> (Option<String>, Option<String>) {
    // Strip the [pid] suffix from the end first
    let id_without_pid = if let Some(bracket_pos) = id.rfind('[') {
        &id[..bracket_pos]
    } else {
        id
    };

    // Direct format: contains comma, split on last comma
    if let Some(last_comma) = id_without_pid.rfind(',') {
        let proc_part = &id_without_pid[..last_comma];
        let actor_part = &id_without_pid[last_comma + 1..];
        if !actor_part.is_empty() {
            return (Some(proc_part.to_string()), Some(actor_part.to_string()));
        }
    }

    // Ranked format: split on last dot
    if let Some(dot_pos) = id_without_pid.rfind('.') {
        let proc_part = &id_without_pid[..dot_pos];
        let actor_part = &id_without_pid[dot_pos + 1..];
        if !actor_part.is_empty() {
            return (Some(proc_part.to_string()), Some(actor_part.to_string()));
        }
    }

    (None, None)
}

/// Format a flight recorder event into a human-readable summary.
///
/// If the event has a "message" field, use that. Otherwise, try to
/// extract meaningful info from other common fields. Falls back to
/// the event name if nothing useful is found.
fn format_event_summary(name: &str, fields: &Value) -> String {
    // Try common field names that contain useful info
    if let Some(obj) = fields.as_object() {
        // "message" is the most common field for readable info
        if let Some(msg) = obj.get("message").and_then(|v| v.as_str()) {
            return msg.to_string();
        }
        // Some events use "msg"
        if let Some(msg) = obj.get("msg").and_then(|v| v.as_str()) {
            return msg.to_string();
        }
        // For handler events, show the handler name
        if let Some(handler) = obj.get("handler").and_then(|v| v.as_str()) {
            return format!("handler: {}", handler);
        }
        // If there are fields but no message, show a compact summary
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
    // Fallback to event name
    name.to_string()
}

/// Format a `serde_json::Value` into a compact, single-line
/// representation.
///
/// This is intended for dense UI surfaces (tables/log panes), where
/// we want a quick "shape" summary rather than pretty-printed JSON:
/// - strings/numbers/bools/null render as their obvious textual
///   forms,
/// - arrays render as `"[N]"` (length only),
/// - objects render as `"{N}"` (number of keys only).
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

/// Return a simple wall-clock timestamp string for UI display.
///
/// This is used for things like the "last refresh" indicator. We
/// intentionally keep it lightweight (HH:MM:SS) and avoid time zone /
/// locale concerns.
///
/// Note: this uses seconds since the Unix epoch modulo 24h, so it’s
/// not a true local-time clock; it's "time-of-day-like" and good
/// enough for a quick status stamp.
#[allow(clippy::disallowed_methods)] // TUI is an external client, doesn't use hyperactor's clock
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

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    if !io::stdout().is_terminal() {
        eprintln!("This TUI requires a real terminal.");
        return Ok(());
    }

    let mut terminal = setup_terminal()?;
    let result = run_app(&mut terminal, args).await;
    restore_terminal(&mut terminal)?;
    result
}

/// Run the main TUI loop.
///
/// Owns the application state (`App`) and drives the render / input /
/// refresh cycle:
/// - redraw the UI into `terminal`
/// - poll for keyboard events and update state
/// - periodically refresh data from the admin API (per
///   `args.refresh_ms`)
///
/// The loop exits when the user requests quit (e.g. via `q`/Esc) or
/// when an unrecoverable terminal I/O error occurs.
async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    args: Args,
) -> io::Result<()> {
    let mut app = App::new(&args.addr);
    let mut refresh_interval = tokio::time::interval(Duration::from_millis(args.refresh_ms));
    let mut events = EventStream::new();

    // Initial fetch
    app.refresh().await;

    loop {
        terminal.draw(|frame| ui(frame, &app))?;

        tokio::select! {
            _ = refresh_interval.tick() => {
                app.refresh().await;
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) => {
                        let old_selected = app.selected;
                        app.on_key(key);
                        // Fetch details if selection changed
                        if app.selected != old_selected {
                            app.fetch_selected_details().await;
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

/// Initialize the terminal for interactive TUI rendering.
///
/// This switches the terminal into "raw mode" (so we get key presses
/// directly), enters the alternate screen buffer (so we don’t trash
/// the user's main shell), and wires up a `ratatui` `Terminal` backed
/// by `crossterm`.
///
/// Pair this with `restore_terminal()` to ensure raw mode is disabled
/// and the alternate screen is exited on shutdown (even on early
/// returns/errors).
fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

/// Restore the terminal to its normal state after running the TUI.
///
/// This is the "undo" for `setup_terminal()`:
/// - leave the alternate screen
/// - disable raw mode
/// - show the cursor again
/// - flush any pending terminal updates
///
/// Call this on all exit paths so the user's shell isn’t left in a
/// broken state.
fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    terminal.backend_mut().execute(LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

/// Render the full UI for the current frame.
///
/// `ui` is the top-level layout function: it splits the available
/// screen area into header / body / footer regions and delegates the
/// actual drawing of each region to helper render functions.
///
/// Keeping this as a thin coordinator makes it easy to tweak the
/// layout without mixing layout concerns into the widgets themselves.
fn ui(frame: &mut ratatui::Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(10),   // Body
            Constraint::Length(2), // Footer
        ])
        .split(frame.area());

    render_header(frame, chunks[0], app);
    render_body(frame, chunks[1], app);
    render_footer(frame, chunks[2]);
}

/// Render the top "status bar" of the UI.
///
/// Shows a fixed title plus a one-line connection/refresh summary (or
/// an error banner if the last refresh failed). This is intentionally
/// lightweight: it gives the operator immediate feedback about
/// whether the UI is connected and how stale the currently displayed
/// data may be.
fn render_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let status = if let Some(err) = &app.error {
        format!("ERROR: {}", err)
    } else {
        format!(
            "Connected to {} | Last refresh: {}",
            app.base_url, app.last_refresh
        )
    };

    let header = Paragraph::new(vec![
        Line::from(Span::styled(
            "Dining Philosophers Monitor",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(status, Style::default().fg(Color::Gray))),
    ])
    .block(Block::default().borders(Borders::BOTTOM));

    frame.render_widget(header, area);
}

/// Render the main two-pane layout.
///
/// The body is split horizontally into:
/// - **Left (tree view):** proc/actor hierarchy navigation.
/// - **Right (details view):** status + recent events for the
///   currently selected actor.
///
/// Keeping this as a small coordinator function makes it easy to
/// tweak the proportions or swap panes without touching the rendering
/// logic inside each.
fn render_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    render_tree(frame, chunks[0], app);
    render_details(frame, chunks[1], app);
}

/// Render the left-hand tree pane.
///
/// `app.tree_nodes` is a flattened, pre-order list of nodes with an
/// associated `depth`. We render it as an indented list with light
/// ASCII branch markers (`├─` / `└─`) to hint at hierarchy without
/// doing a full tree layout pass.
///
/// Styling rules:
/// - Selected row is highlighted for keyboard navigation.
/// - Depth-0 rows (proc roots) are styled distinctly from actor rows.
///
/// Note: the "branch marker" heuristic is intentionally simple; it
/// looks at the next node’s depth to decide whether this node is the
/// last sibling.
fn render_tree(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let items: Vec<ListItem> = app
        .tree_nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| {
            let indent = "  ".repeat(node.depth);
            let prefix = if node.depth == 0 {
                ""
            } else if idx + 1 < app.tree_nodes.len() && app.tree_nodes[idx + 1].depth >= node.depth
            {
                "├─ "
            } else {
                "└─ "
            };

            let style = if idx == app.selected {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else if node.depth == 0 {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::White)
            };

            ListItem::new(Line::from(Span::styled(
                format!("{}{}{}", indent, prefix, node.label),
                style,
            )))
        })
        .collect();

    let block = Block::default()
        .title("Actor Tree")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

/// Render the right-hand details pane for the currently selected
/// actor.
///
/// The details pane is vertically split into:
/// - **Actor info**: a compact summary (type, status, counters,
///   timestamps, etc.).
/// - **Flight recorder**: a scrolling-ish list of recent events (tail
///   of the ring buffer).
///
/// When no actor is selected or the fetch fails, this pane displays
/// either a placeholder or the last error captured in
/// `app.detail_error`.
fn render_details(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(5)])
        .split(area);

    // Actor info panel
    let info_block = Block::default()
        .title("Actor Details")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let info_content = if let Some(details) = &app.actor_details {
        vec![
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
                Span::raw(format!("{}us", details.total_processing_time_us)),
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
        ]
    } else if let Some(err) = &app.detail_error {
        vec![Line::from(Span::styled(
            err.as_str(),
            Style::default().fg(Color::Red),
        ))]
    } else if app
        .tree_nodes
        .get(app.selected)
        .is_none_or(|n| n.actor_name.is_none())
    {
        vec![Line::from(Span::styled(
            "Select an actor to view details",
            Style::default().fg(Color::Gray),
        ))]
    } else {
        vec![Line::from(Span::raw("Loading..."))]
    };

    let info = Paragraph::new(info_content).block(info_block);
    frame.render_widget(info, chunks[0]);

    // Flight recorder panel
    let recorder_block = Block::default()
        .title("Flight Recorder")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Gray));

    let recorder_content: Vec<Line> = if let Some(details) = &app.actor_details {
        details
            .flight_recorder
            .iter()
            .rev()
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
                        format!("{} ", &event.timestamp[11..19]), // Just time portion
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::raw(format_event_summary(&event.name, &event.fields)),
                ])
            })
            .collect()
    } else {
        vec![Line::from(Span::styled(
            "No events",
            Style::default().fg(Color::Gray),
        ))]
    };

    let recorder = Paragraph::new(recorder_content)
        .block(recorder_block)
        .wrap(Wrap { trim: true });
    frame.render_widget(recorder, chunks[1]);
}

/// Render the bottom help/footer bar.
///
/// This is a static "key legend" to make the UI self-discoverable
/// when you drop into it via `hyper`, `buck run`, or a random
/// container shell. It keeps the primary navigation shortcuts visible
/// without consuming much space.
fn render_footer(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let help = "q/Esc: quit | j/k or Up/Down: navigate | Home/End: jump";
    let footer = Paragraph::new(help)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, area);
}
