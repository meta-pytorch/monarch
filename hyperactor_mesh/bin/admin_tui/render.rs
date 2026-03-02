/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

// TODO(admin_tui_step2): replace &App with AppView or UiModel.
// Render reads a subset of App state; update this list while moving code:
// - base_url
// - cursor
// - detail
// - detail_error
// - error
// - lang_name
// - refresh_interval_label
// - show_stopped
// - show_system
// - theme
// - theme_name
// - tree_scroll_offset
// Keep this list accurate; it bounds Step 2.

pub mod detail_pane;
pub mod status_bar;
pub mod tree_pane;

use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;

use self::detail_pane::render_detail_pane;
use self::status_bar::render_footer;
use self::status_bar::render_header;
use self::tree_pane::render_topology_tree;
use crate::App;

/// Render a full frame of the TUI.
///
/// Splits the screen into header/body/footer regions and delegates to
/// the corresponding render helpers.
pub(crate) fn ui(frame: &mut ratatui::Frame<'_>, app: &App) {
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
    render_footer(frame, chunks[2], &app.theme.scheme, &app.theme.labels);
}

/// Render the main body of the UI.
///
/// Splits the screen into a left topology pane and a right detail
/// pane, and renders each using the current application state.
pub(crate) fn render_body(frame: &mut ratatui::Frame<'_>, area: ratatui::layout::Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    render_topology_tree(frame, chunks[0], app);
    render_detail_pane(frame, chunks[1], app);
}
