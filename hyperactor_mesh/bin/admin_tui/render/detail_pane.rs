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
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use ratatui::widgets::Block;
use ratatui::widgets::Borders;
use ratatui::widgets::Paragraph;
use ratatui::widgets::Wrap;

use crate::App;
use crate::diagnostics::DiagNodeRole;
use crate::diagnostics::DiagOutcome;
use crate::diagnostics::DiagPhase;
use crate::diagnostics::DiagResult;
use crate::diagnostics::DiagSummary;
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
    if app.diag_running || !app.diag_results.is_empty() {
        render_diagnostics_pane(frame, area, app);
        return;
    }
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
            Span::styled(child, scheme.node_host),
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
            .map(|pid| pid.name().to_string())
            .unwrap_or_else(|_| child.clone());
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(short, scheme.node_proc),
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
        .constraints([Constraint::Length(info_height), Constraint::Min(5)])
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
        vec![Line::from(Span::styled("No events", scheme.detail_label))]
    } else {
        recorded_events
            .iter()
            .take(20)
            .map(|event| {
                let level_style = match event.level.as_str() {
                    "ERROR" => scheme.error,
                    "WARN" => scheme.detail_status_warn,
                    "INFO" => scheme.detail_status_ok,
                    "DEBUG" => scheme.info,
                    _ => scheme.detail_label,
                };
                Line::from(vec![
                    Span::styled(
                        format!("{} ", event.level.chars().next().unwrap_or('?')),
                        level_style,
                    ),
                    Span::styled(
                        format!("{} ", format_local_time(&event.timestamp)),
                        scheme.detail_label,
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

/// Render the live self-diagnostic pane.
///
/// Shows phase-separated probe results as they stream in. While the
/// run is still in progress a "Running…" indicator is shown; once
/// complete a summary line reports overall health.
pub(crate) fn render_diagnostics_pane(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let scheme = &app.theme.scheme;
    let labels = &app.theme.labels;
    let results = &app.diag_results;

    // Build the summary/status line that is pinned above the scrollable results.
    let sep = labels.separator;
    let status_line = if app.diag_running {
        Line::from(vec![
            Span::styled(labels.diag_running, scheme.info),
            Span::styled(
                format!("{}{}", sep, labels.diag_live_updates),
                scheme.detail_label,
            ),
        ])
    } else if !results.is_empty() {
        let s = DiagSummary::from_results(results);
        let admin_status = if s.admin_passed == s.admin_total && s.admin_total > 0 {
            labels.diag_status_healthy
        } else {
            labels.diag_status_failing
        };
        let mesh_status = if s.mesh_total == 0 {
            labels.diag_status_na
        } else if s.mesh_passed == s.mesh_total {
            labels.diag_status_healthy
        } else {
            labels.diag_status_failing
        };
        let (summary, summary_style) = if !s.any_fail {
            (
                format!(
                    "{} {} {}. {} {} {} {}.",
                    labels.diag_checks_all,
                    s.total,
                    labels.diag_checks_passed,
                    labels.diag_admin_label,
                    admin_status,
                    labels.diag_mesh_label,
                    mesh_status,
                ),
                scheme.detail_status_ok,
            )
        } else {
            (
                format!(
                    "{}/{} {}. {} {}/{}. {} {}/{}.",
                    s.passed,
                    s.total,
                    labels.diag_checks_passed,
                    labels.diag_admin_label,
                    s.admin_passed,
                    s.admin_total,
                    labels.diag_mesh_label,
                    s.mesh_passed,
                    s.mesh_total,
                ),
                scheme.error,
            )
        };
        Line::from(Span::styled(summary, summary_style))
    } else {
        Line::default()
    };

    // Scrollable section results.
    let sep_style = Style::default().add_modifier(Modifier::BOLD);
    let mut lines: Vec<Line<'static>> = Vec::new();

    lines.push(Line::from(Span::styled(
        "── Admin Infra ──────────────────────────────────",
        sep_style,
    )));
    for r in results.iter().filter(|r| r.phase == DiagPhase::AdminInfra) {
        lines.push(diag_result_line(r, scheme, labels));
    }
    lines.push(Line::default());
    lines.push(Line::from(Span::styled(
        "── Mesh ─────────────────────────────────────────",
        sep_style,
    )));
    for r in results.iter().filter(|r| r.phase == DiagPhase::Mesh) {
        lines.push(diag_result_line(r, scheme, labels));
    }

    // Render the outer block, then split the inner area into a pinned
    // status row and the scrollable results below it.
    let title_line = if app.diag_running {
        Line::from(vec![
            Span::styled(
                labels.pane_diagnostics,
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!("{}{}", sep, labels.diag_running), scheme.info),
        ])
    } else if let Some(t) = &app.diag_completed_at {
        Line::from(vec![
            Span::styled(
                labels.pane_diagnostics,
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("{}{}", sep, labels.diag_completed_at),
                scheme.detail_label,
            ),
            Span::raw(" "),
            Span::styled(t.clone(), scheme.stat_timing),
            Span::styled(
                format!("{}{}", sep, labels.diag_static_snapshot),
                scheme.detail_stopped,
            ),
        ])
    } else {
        Line::from(Span::raw(labels.pane_diagnostics))
    };
    let block = Block::default()
        .title(title_line)
        .borders(Borders::ALL)
        .border_style(scheme.border);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Split inner area: 1 row for pinned status, rest for scrollable results.
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(inner);

    frame.render_widget(Paragraph::new(status_line), chunks[0]);

    // Clamp scroll against the scrollable chunk height.
    let max_scroll = (lines.len() as u16).saturating_sub(chunks[1].height);
    app.diag_max_scroll.set(max_scroll);
    let scroll = app.diag_scroll.get().min(max_scroll);
    app.diag_scroll.set(scroll);

    let results_p = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    frame.render_widget(results_p, chunks[1]);
}

/// Format one diagnostic probe result as a TUI row.
fn diag_result_line(r: &DiagResult, scheme: &ColorScheme, labels: &Labels) -> Line<'static> {
    let (icon, icon_style) = match &r.outcome {
        DiagOutcome::Pass { .. } => ("✓", scheme.detail_status_ok),
        DiagOutcome::Slow { .. } => ("⚠", scheme.detail_status_warn),
        DiagOutcome::Fail { .. } => ("✗", scheme.error),
    };
    let timing = match &r.outcome {
        DiagOutcome::Pass { elapsed_ms } | DiagOutcome::Slow { elapsed_ms } => {
            format!(" {}ms", elapsed_ms)
        }
        DiagOutcome::Fail { elapsed_ms, error } => format!(" {}ms — {}", elapsed_ms, error),
    };
    let mut spans = vec![
        Span::styled(format!(" {} ", icon), icon_style),
        Span::raw(r.label.clone()),
        Span::styled(timing, scheme.detail_label),
    ];
    if let Some(role) = r.note {
        let note = match role {
            DiagNodeRole::AdminServer => labels.diag_note_admin_server,
            DiagNodeRole::HostAgent => labels.diag_note_host_agent,
            DiagNodeRole::AdminServiceProc => labels.diag_note_admin_service_proc,
            DiagNodeRole::LocalClientProc => labels.diag_note_local_client_proc,
            DiagNodeRole::IntrospectionHandler => labels.diag_note_introspection_handler,
            DiagNodeRole::ActorLifecycleManager => labels.diag_note_actor_lifecycle_manager,
            DiagNodeRole::RootClientBridge => labels.diag_note_root_client_bridge,
            DiagNodeRole::CommActor => labels.diag_note_comm_actor,
            DiagNodeRole::ProcAgent => labels.diag_note_proc_agent,
            DiagNodeRole::UserProc => labels.diag_note_user_proc,
            DiagNodeRole::UserActor => labels.diag_note_user_actor,
        };
        spans.push(Span::styled(format!("  — {}", note), scheme.stat_url));
    }
    Line::from(spans)
}
