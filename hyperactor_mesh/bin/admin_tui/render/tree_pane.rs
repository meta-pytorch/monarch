/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::List;
use ratatui::widgets::ListItem;
use ratatui::widgets::ListState;

use crate::App;

/// Render the topology tree (left pane).
///
/// Uses `visible_rows()` to display only expanded nodes. Each row
/// includes indentation/connectors, an expand/collapse glyph for
/// nodes with children, and color-coding by `NodeType`, with the
/// selected row highlighted.
pub(crate) fn render_topology_tree(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let rows = app.visible_rows();
    let scheme = &app.theme.scheme;

    // When the diagnostics overlay is active the tree is non-interactive.
    // Render it uniformly dim so the user can see it is inactive.
    let diag_active = app.diag_running || !app.diag_results.is_empty();

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

            // Style precedence: diag-inactive > selected > failed > stopped >
            // system > node-type.  When diag_active the entire pane is dimmed
            // and the selection/failed/system colours must not bleed through.
            let style = if diag_active {
                scheme.detail_stopped
            } else if vis_idx == app.cursor.pos() {
                scheme.stat_selection.add_modifier(Modifier::BOLD)
            } else if node.failed {
                scheme.node_failed
            } else if node.stopped {
                scheme.detail_stopped
            } else if node.is_system {
                scheme.node_system_actor
            } else {
                scheme.node_user_actor
            };

            // Hide the selection caret while the pane is inactive.
            let marker = if !diag_active && vis_idx == app.cursor.pos() {
                app.theme.labels.selection_caret
            } else {
                "  "
            };

            ListItem::new(Line::from(Span::styled(
                format!("{}{}{}{}{}", marker, indent, connector, fold, node.label),
                style,
            )))
        })
        .collect();

    let block = if diag_active {
        Block::default()
            .title(Span::styled(
                app.theme.labels.pane_topology,
                scheme.detail_stopped,
            ))
            .borders(Borders::ALL)
            .border_style(scheme.detail_stopped)
    } else {
        Block::default()
            .title(app.theme.labels.pane_topology)
            .borders(Borders::ALL)
            .border_style(scheme.border)
    };

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default());
    let mut list_state = ListState::default()
        .with_selected(Some(app.cursor.pos()))
        .with_offset(app.tree_scroll_offset);
    frame.render_stateful_widget(list, area, &mut list_state);
}
