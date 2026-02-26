/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::str::FromStr;

use hyperactor::ProcId;
use hyperactor::introspect::FailureInfo;
use hyperactor::introspect::NodePayload;
use hyperactor::introspect::NodeProperties;
use hyperactor::introspect::RecordedEvent;
use ratatui::layout::Constraint;
use ratatui::layout::Direction;
use ratatui::layout::Layout;
use ratatui::layout::Rect;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Wrap;

use crate::App;
use crate::format::format_event_summary;
use crate::format::format_local_time;
use crate::format::format_relative_time;
use crate::format::format_uptime;
use crate::theme::ColorScheme;
use crate::theme::Labels;

/// Render the contextual details pane (right side).
///
/// If a `NodePayload` for the current selection is available in
/// `app.detail`, dispatches to `render_node_detail` to show a
/// type-specific view (root/host/proc/actor). Otherwise, shows either
/// the last fetch error (`app.detail_error`) or a neutral "select a
/// node" placeholder message.
pub(crate) fn render_detail_pane(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    match &app.detail {
        Some(payload) => {
            render_node_detail(frame, area, payload, &app.theme.scheme, &app.theme.labels)
        }
        None => {
            let msg = app
                .detail_error
                .as_deref()
                .unwrap_or("Select a node to view details");
            let msg_style = if app.detail_error.is_some() {
                app.theme.scheme.error
            } else {
                app.theme.scheme.info
            };
            let block = Block::default()
                .title(app.theme.labels.pane_details)
                .borders(Borders::ALL)
                .border_style(app.theme.scheme.border);
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
pub(crate) fn render_node_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    scheme: &ColorScheme,
    labels: &Labels,
) {
    match &payload.properties {
        NodeProperties::Root {
            num_hosts,
            started_at,
            started_by,
            ..
        } => {
            render_root_detail(
                frame, area, payload, *num_hosts, started_at, started_by, scheme, labels,
            );
        }
        NodeProperties::Host {
            addr, num_procs, ..
        } => {
            render_host_detail(frame, area, payload, addr, *num_procs, scheme, labels);
        }
        NodeProperties::Proc {
            proc_name,
            num_actors,
            is_poisoned,
            failed_actor_count,
            ..
        } => {
            render_proc_detail(
                frame,
                area,
                payload,
                proc_name,
                *num_actors,
                *is_poisoned,
                *failed_actor_count,
                scheme,
                labels,
            );
        }
        NodeProperties::Actor {
            actor_status,
            actor_type,
            messages_processed,
            created_at,
            last_message_handler,
            total_processing_time_us,
            flight_recorder,
            failure_info,
            ..
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
                failure_info.as_ref(),
                scheme,
                labels,
            );
        }
        NodeProperties::Error { code, message } => {
            let text = format!("Error: {} — {}", code, message);
            let paragraph = Paragraph::new(text)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .border_style(scheme.border)
                        .title(labels.pane_error),
                )
                .style(scheme.error)
                .wrap(Wrap { trim: true });
            frame.render_widget(paragraph, area);
        }
    }
}

/// Build a key-value detail line with a gray label and raw value.
pub(crate) fn detail_line<'a>(
    label: &'a str,
    value: impl Into<String>,
    scheme: &ColorScheme,
) -> Line<'a> {
    Line::from(vec![
        Span::styled(label, scheme.detail_label),
        Span::raw(value.into()),
    ])
}

/// Render the right-pane detail view for the mesh root node.
///
/// Shows a simple summary (host count) and then lists the root's
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
    l: &Labels,
) {
    let block = Block::default()
        .title(l.pane_root_details)
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let uptime_str = format_uptime(started_at);

    let mut lines = vec![
        detail_line(l.hosts, num_hosts.to_string(), scheme),
        detail_line(l.started_by, started_by, scheme),
        detail_line(l.uptime_detail, &uptime_str, scheme),
        detail_line(l.started_at, format_local_time(started_at), scheme),
        detail_line(l.data_as_of, format_relative_time(&payload.as_of), scheme),
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
/// Displays the host's address and proc count, then lists the host's
/// proc children using a shortened proc name for readability.
fn render_host_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    addr: &str,
    num_procs: usize,
    scheme: &ColorScheme,
    l: &Labels,
) {
    let block = Block::default()
        .title(l.pane_host_details)
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let mut lines = vec![
        detail_line(l.address, addr, scheme),
        detail_line(l.procs, num_procs.to_string(), scheme),
        detail_line(l.data_as_of, format_relative_time(&payload.as_of), scheme),
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
    is_poisoned: bool,
    failed_actor_count: usize,
    scheme: &ColorScheme,
    l: &Labels,
) {
    let block = Block::default()
        .title(l.pane_proc_details)
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let mut lines = vec![
        detail_line(l.name, proc_name, scheme),
        detail_line(l.actors, num_actors.to_string(), scheme),
        detail_line(l.data_as_of, format_relative_time(&payload.as_of), scheme),
    ];

    if is_poisoned {
        lines.push(Line::from(vec![
            Span::styled(l.poisoned, scheme.node_failed),
            Span::styled(l.yes, scheme.node_failed),
        ]));
        lines.push(Line::from(vec![
            Span::styled(l.failed_actors, scheme.node_failed),
            Span::styled(failed_actor_count.to_string(), scheme.node_failed),
        ]));
    }

    lines.push(Line::default());
    for (i, actor) in payload.children.iter().enumerate() {
        if i >= 50 {
            lines.push(Line::from(Span::styled(
                format!("  ... and {} more", payload.children.len() - 50),
                scheme.detail_stopped,
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
    failure_info: Option<&FailureInfo>,
    scheme: &ColorScheme,
    l: &Labels,
) {
    let info_height = if failure_info.is_some() { 15 } else { 11 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(info_height), Constraint::Length(5)])
        .split(area);

    // Actor info
    let info_block = Block::default()
        .title(l.pane_actor_details)
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let status_style = if failure_info.is_some() {
        scheme.detail_status_failed
    } else if actor_status == "Running" {
        scheme.detail_status_ok
    } else {
        scheme.detail_status_warn
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled(l.status, scheme.detail_label),
            Span::styled(actor_status, status_style),
        ]),
        detail_line(l.data_as_of, format_relative_time(&payload.as_of), scheme),
        detail_line(l.actor_type, actor_type, scheme),
        detail_line(l.messages, messages_processed.to_string(), scheme),
        detail_line(
            l.processing_time,
            humantime::format_duration(std::time::Duration::from_micros(total_processing_time_us))
                .to_string(),
            scheme,
        ),
        detail_line(l.created, created_at, scheme),
        detail_line(l.last_handler, last_message_handler.unwrap_or("-"), scheme),
        detail_line(l.children, payload.children.len().to_string(), scheme),
    ];

    if let Some(fi) = failure_info {
        lines.push(Line::default());
        lines.push(Line::from(vec![
            Span::styled(l.error_message, scheme.node_failed),
            Span::styled(&fi.error_message, scheme.node_failed),
        ]));
        let root_cause_display = match &fi.root_cause_name {
            Some(name) => format!("{} ({})", name, fi.root_cause_actor),
            None => fi.root_cause_actor.clone(),
        };
        lines.push(Line::from(vec![
            Span::styled(l.root_cause, scheme.detail_label),
            Span::raw(root_cause_display),
        ]));
        lines.push(detail_line(l.failed_at, &fi.occurred_at, scheme));
        lines.push(Line::from(vec![
            Span::styled(l.propagated, scheme.detail_label),
            Span::raw(if fi.is_propagated { l.yes } else { l.no }),
        ]));
    }

    let info = Paragraph::new(lines)
        .block(info_block)
        .wrap(Wrap { trim: false });
    frame.render_widget(info, chunks[0]);

    // Flight recorder
    let recorder_block = Block::default()
        .title(l.pane_flight_recorder)
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
