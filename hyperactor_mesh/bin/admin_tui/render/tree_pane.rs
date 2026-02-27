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

            // Style precedence: selected > failed > stopped > system > node-type.
            // Failed actors render red; stopped actors render gray.
            let style = if vis_idx == app.cursor.pos() {
                app.theme.scheme.stat_selection.add_modifier(Modifier::BOLD)
            } else if node.failed {
                app.theme.scheme.node_failed
            } else if node.stopped {
                app.theme.scheme.detail_stopped
            } else if node.is_system {
                app.theme.scheme.node_system_actor
            } else {
                app.theme.scheme.node_style(node.node_type)
            };

            let marker = if vis_idx == app.cursor.pos() {
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

    let block = Block::default()
        .title(app.theme.labels.pane_topology)
        .borders(Borders::ALL)
        .border_style(app.theme.scheme.border);

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default());
    let mut list_state = ListState::default()
        .with_selected(Some(app.cursor.pos()))
        .with_offset(app.tree_scroll_offset);
    frame.render_stateful_widget(list, area, &mut list_state);
}
