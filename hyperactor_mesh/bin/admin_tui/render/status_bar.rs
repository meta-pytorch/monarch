/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use hyperactor::introspect::NodeProperties;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::Paragraph;

use crate::App;
use crate::format::format_uptime;
use crate::model::NodeType;
use crate::theme::ColorScheme;
use crate::theme::Labels;
use crate::theme::LangName;
use crate::theme::ThemeName;

/// Render the top status/header bar.
///
/// Displays a colorful, information-dense header with topology stats,
/// selection context, and system state. Uses semantic colors from the
/// scheme for visual hierarchy and readability.
pub(crate) fn render_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let l = &app.theme.labels;

    // Error state overrides normal display
    if let Some(err) = &app.error {
        let header = Paragraph::new(vec![
            Line::from(Span::styled(l.app_name, app.theme.scheme.app_name)),
            Line::from(Span::styled(
                format!("ERROR: {}", err),
                app.theme.scheme.error,
            )),
        ])
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(app.theme.scheme.border),
        );
        frame.render_widget(header, area);
        return;
    }

    // Gather stats
    let selection = app.current_selection();
    let sys_state = if app.show_system { l.sys_on } else { l.sys_off };
    let stopped_state = if app.show_stopped {
        l.stopped_on
    } else {
        l.stopped_off
    };

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

    // Line 1: App name • URL • up: 2h 34m • @user • sys:off • ⟳ 1s
    let mut line1_spans = vec![
        Span::styled(l.app_name, app.theme.scheme.app_name),
        Span::styled(l.separator, app.theme.scheme.stat_label),
        Span::styled(&app.base_url, app.theme.scheme.stat_url),
    ];

    // Add uptime if available
    if let Some(uptime) = uptime_str {
        line1_spans.extend(vec![
            Span::styled(l.separator, app.theme.scheme.stat_label),
            Span::styled(l.uptime, app.theme.scheme.stat_label),
            Span::styled(uptime, app.theme.scheme.stat_timing),
        ]);
    }

    // Add username if available
    if let Some(user) = username {
        line1_spans.extend(vec![
            Span::styled(l.separator, app.theme.scheme.stat_label),
            Span::styled(format!("@{}", user), app.theme.scheme.stat_system),
        ]);
    }

    line1_spans.extend(vec![
        Span::styled(l.separator, app.theme.scheme.stat_label),
        Span::styled(l.system, app.theme.scheme.stat_label),
        Span::styled(sys_state, app.theme.scheme.stat_system),
    ]);

    line1_spans.extend(vec![
        Span::styled(l.separator, app.theme.scheme.stat_label),
        Span::styled(l.stopped, app.theme.scheme.stat_label),
        Span::styled(stopped_state, app.theme.scheme.stat_system),
    ]);

    // Show active theme and lang (skip defaults to reduce noise)
    if !matches!(app.theme_name, ThemeName::Nord) {
        line1_spans.extend(vec![
            Span::styled(l.separator, app.theme.scheme.stat_label),
            Span::styled(
                format!("theme:{}", app.theme_name),
                app.theme.scheme.stat_system,
            ),
        ]);
    }
    if !matches!(app.lang_name, LangName::En) {
        line1_spans.extend(vec![
            Span::styled(l.separator, app.theme.scheme.stat_label),
            Span::styled(
                format!("lang:{}", app.lang_name),
                app.theme.scheme.stat_system,
            ),
        ]);
    }

    if !app.refresh_interval_label.is_empty() {
        line1_spans.extend(vec![
            Span::styled(l.separator, app.theme.scheme.stat_label),
            Span::styled(l.refresh_icon, app.theme.scheme.stat_timing),
            Span::styled(&app.refresh_interval_label, app.theme.scheme.stat_timing),
        ]);
    }

    // Line 2: Selection context
    let mut line2_spans = vec![];

    if let Some(node) = selection {
        let type_str = node.node_type.label();
        let type_style = app.theme.scheme.node_style(node.node_type);

        line2_spans.extend(vec![
            Span::styled(l.selection_caret, app.theme.scheme.stat_selection),
            Span::styled(type_str, type_style),
            Span::styled(" ", Style::default()),
            Span::styled(&node.label, app.theme.scheme.stat_selection),
        ]);

        // Classification tag only for actors (system/failed/stopped/live)
        if matches!(node.node_type, NodeType::Actor) {
            let (class_label, class_style) = if node.failed {
                ("failed", app.theme.scheme.node_failed)
            } else if node.stopped {
                ("stopped", app.theme.scheme.detail_stopped)
            } else if node.is_system {
                ("system", app.theme.scheme.node_system_actor)
            } else {
                ("live", type_style)
            };
            line2_spans.extend(vec![
                Span::styled(" [", app.theme.scheme.header_class_bracket),
                Span::styled(class_label, class_style),
                Span::styled("]", app.theme.scheme.header_class_bracket),
            ]);
        }
    } else {
        line2_spans.push(Span::styled(l.no_selection, app.theme.scheme.info));
    }

    let header = Paragraph::new(vec![Line::from(line1_spans), Line::from(line2_spans)]).block(
        Block::default()
            .borders(Borders::BOTTOM)
            .border_style(app.theme.scheme.border),
    );

    frame.render_widget(header, area);
}

/// Render the bottom help bar showing the keyboard shortcuts.
///
/// This is a static hint line
/// (quit/navigation/expand-collapse/filter) separated from the main
/// UI with a top border so it reads like a persistent status/help
/// footer.
pub(crate) fn render_footer(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    scheme: &ColorScheme,
    labels: &Labels,
) {
    let footer = Paragraph::new(labels.footer_help_text)
        .style(scheme.footer_help)
        .block(Block::default().borders(Borders::TOP));
    frame.render_widget(footer, area);
}
