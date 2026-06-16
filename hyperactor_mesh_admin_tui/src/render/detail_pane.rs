/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::SystemTime;

use hyperactor::introspect::RecordedEvent;
use hyperactor_mesh::introspect::Execution;
use hyperactor_mesh::introspect::FailureInfo;
use hyperactor_mesh::introspect::InboundOrdering;
use hyperactor_mesh::introspect::NodePayload;
use hyperactor_mesh::introspect::NodeProperties;
use hyperactor_mesh::introspect::NodeRef;
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
use crate::Theme;
use crate::diagnostics::DiagNodeRole;
use crate::diagnostics::DiagOutcome;
use crate::diagnostics::DiagPhase;
use crate::diagnostics::DiagResult;
use crate::diagnostics::DiagSummary;
use crate::format::format_event_summary;
use crate::format::format_local_time;
use crate::format::format_system_time_iso;
use crate::format::format_system_time_local;
use crate::format::format_system_time_relative;
use crate::format::format_system_time_uptime;
use crate::format_bytes;
use crate::theme::ColorScheme;
use crate::theme::Labels;

/// Render the contextual details pane (right side).
///
/// Precedence: the help glossary (`app.show_help`, TUI-22) takes priority,
/// then the active `app.overlay` (py-spy / config / diagnostics), then node
/// detail. For node detail, if a `NodePayload` for the current selection is
/// available in `app.detail`, dispatches to `render_node_detail` to show a
/// type-specific view (root/host/proc/actor); otherwise shows either the
/// last fetch error (`app.detail_error`) or a neutral "select a node"
/// placeholder message.
pub(crate) fn render_detail_pane(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    if app.show_help {
        render_help_overlay(frame, area, app.detail.as_ref(), &app.theme.scheme);
        return;
    }
    if let Some(overlay) = &app.overlay {
        render_overlay(frame, area, overlay, &app.theme.scheme);
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

/// A single field-help entry: a field name, its one-line meaning, and an
/// optional subdued note line for caveats.
pub(crate) struct HelpEntry {
    pub(crate) field: &'static str,
    pub(crate) meaning: &'static str,
    pub(crate) note: Option<&'static str>,
}

const fn help(field: &'static str, meaning: &'static str) -> HelpEntry {
    HelpEntry {
        field,
        meaning,
        note: None,
    }
}

const fn noted(field: &'static str, meaning: &'static str, note: &'static str) -> HelpEntry {
    HelpEntry {
        field,
        meaning,
        note: Some(note),
    }
}

// First-pass glossary is intentionally English-only. Move into localized
// help topics if/when the feature is promoted.
//
// This is a glossary of the non-obvious fields, not exhaustive row coverage:
// one entry per confusing rendered row, at that row's granularity. Field
// names are canonical English, independent of the localized `Labels`.
pub(crate) fn help_content(props: &NodeProperties) -> (&'static str, &'static [HelpEntry]) {
    // Entries live in `const` items so the returned slices are `'static`
    // (a `&[..]` temporary built in a match arm is not promoted and would
    // not outlive the function).
    const ROOT: &[HelpEntry] = &[
        help("hosts", "number of host processes in the mesh"),
        help("started by", "identity that launched this root"),
        help("uptime", "time since this root was started"),
    ];
    const HOST: &[HelpEntry] = &[
        help("address", "network address of this host process"),
        help("procs", "number of worker processes on this host"),
        help("rss", "resident set size: physical RAM used by the process"),
        help("vm size", "virtual address space reserved by the process"),
    ];
    const PROC: &[HelpEntry] = &[
        help("actors", "live actors running in this process"),
        help("rss", "resident set size: physical RAM used by the process"),
        help("vm size", "virtual address space reserved by the process"),
        noted(
            "queue depth",
            "accepted handler work not yet dequeued (total across actors; max = largest single actor)",
            "max is a point-in-time snapshot, not a historical high-water mark",
        ),
        help(
            "peak depth",
            "maximum proc-wide queue total observed since startup",
        ),
        help(
            "last busy",
            "time since proc queue total was last observed non-zero",
        ),
        help(
            "poisoned",
            "proc has failed and should no longer be treated as healthy",
        ),
    ];
    const ACTOR: &[HelpEntry] = &[
        help(
            "status",
            "current actor lifecycle state: ok / degraded / failed",
        ),
        help("instance", "stable UUID for this actor instance"),
        noted(
            "queue depth",
            "accepted handler work not yet dequeued by this actor",
            "includes out-of-order work held by the reorder buffer",
        ),
        help("msgs processed", "messages handled since actor start"),
        help(
            "processing time",
            "cumulative time spent in message handlers",
        ),
        help("last handler", "most recent handler method invoked"),
        help("children", "actors spawned by this actor"),
        help(
            "ordering enabled",
            "whether inbound sequence tracking is active",
        ),
        help(
            "sessions stalled",
            "sender sessions waiting on a missing next sequence",
        ),
        noted(
            "buffered",
            "out-of-order frames held by inbound ordering",
            "independent diagnostic; do not compare arithmetically with queue depth",
        ),
    ];
    const ERROR: &[HelpEntry] = &[
        help("code", "error code returned when resolving this node"),
        help("message", "error detail"),
    ];
    match props {
        NodeProperties::Root { .. } => ("root", ROOT),
        NodeProperties::Host { .. } => ("host", HOST),
        NodeProperties::Proc { .. } => ("proc", PROC),
        NodeProperties::Actor { .. } => ("actor", ACTOR),
        NodeProperties::Error { .. } => ("error", ERROR),
    }
}

/// Render the static help glossary overlay (TUI-22) into the detail pane.
///
/// Read-only: lists field meanings for the selected node kind, with the
/// kind shown in the block title (e.g. " ? actor help "). Non-scrollable;
/// content is kept short enough to fit common pane heights.
fn render_help_overlay(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: Option<&NodePayload>,
    scheme: &ColorScheme,
) {
    // Title carries the node kind for context, e.g. " ? actor help ".
    let (title, lines): (String, Vec<Line<'static>>) = match payload {
        None => (
            " ? help ".to_string(),
            vec![
                Line::from(Span::styled(
                    "Select a node to view field help",
                    scheme.info,
                )),
                Line::raw(""),
                Line::from(Span::styled("press any key to dismiss", scheme.info)),
            ],
        ),
        Some(p) => {
            let (kind, entries) = help_content(&p.properties);
            let mut lines = Vec::new();
            for e in entries {
                lines.push(Line::from(vec![
                    Span::styled(format!("{:<22}", e.field), scheme.detail_label),
                    Span::raw(e.meaning),
                ]));
                if let Some(n) = e.note {
                    lines.push(Line::from(vec![
                        Span::raw(format!("{:<22}", "")),
                        Span::styled(n, scheme.info),
                    ]));
                }
            }
            lines.push(Line::raw(""));
            lines.push(Line::from(Span::styled(
                "press any key to dismiss",
                scheme.info,
            )));
            (format!(" ? {kind} help "), lines)
        }
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(scheme.border);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
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
            addr,
            num_procs,
            memory,
            ..
        } => {
            render_host_detail(
                frame, area, payload, addr, *num_procs, memory, scheme, labels,
            );
        }
        NodeProperties::Proc {
            proc_name,
            num_actors,
            is_poisoned,
            failed_actor_count,
            debug,
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
                debug,
                scheme,
                labels,
            );
        }
        NodeProperties::Actor {
            actor_status,
            actor_type,
            instance_id,
            messages_processed,
            queue_depth,
            created_at,
            last_message_handler,
            total_processing_time_us,
            flight_recorder,
            inbound_ordering,
            execution,
            failure_info,
            ..
        } => {
            render_actor_detail(
                frame,
                area,
                payload,
                actor_status,
                actor_type,
                instance_id,
                *queue_depth,
                *messages_processed,
                created_at,
                last_message_handler.as_deref(),
                *total_processing_time_us,
                flight_recorder.as_deref(),
                inbound_ordering.as_deref(),
                execution.as_deref(),
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
    started_at: &SystemTime,
    started_by: &str,
    scheme: &ColorScheme,
    l: &Labels,
) {
    let block = Block::default()
        .title(l.pane_root_details)
        .borders(Borders::ALL)
        .border_style(scheme.border);

    let uptime_str = format_system_time_uptime(started_at);

    let mut lines = vec![
        detail_line(l.hosts, num_hosts.to_string(), scheme),
        detail_line(l.started_by, started_by, scheme),
        detail_line(l.uptime_detail, &uptime_str, scheme),
        detail_line(l.started_at, format_system_time_local(started_at), scheme),
        detail_line(
            l.data_as_of,
            format_system_time_relative(&payload.as_of),
            scheme,
        ),
        Line::default(),
    ];
    for child in &payload.children {
        let child_str = child.to_string();
        lines.push(Line::from(vec![
            Span::styled("  ", Style::default()),
            Span::styled(child_str, scheme.node_host),
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
    memory: &hyperactor_mesh::introspect::ProcessMemoryStats,
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
    ];
    lines.push(detail_line(
        l.rss,
        memory
            .process_rss_bytes
            .map_or_else(|| "N/A".to_string(), format_bytes),
        scheme,
    ));
    lines.push(detail_line(
        l.vm_size,
        memory
            .process_vm_size_bytes
            .map_or_else(|| "N/A".to_string(), format_bytes),
        scheme,
    ));
    lines.push(detail_line(
        l.data_as_of,
        format_system_time_relative(&payload.as_of),
        scheme,
    ));
    lines.push(Line::default());
    for child in &payload.children {
        let child_str = child.to_string();
        let short = match child {
            NodeRef::Proc(proc_id) => proc_id.id().to_string(),
            _ => child_str.clone(),
        };
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
    debug: &hyperactor_mesh::introspect::ProcDebugStats,
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
    ];
    // Hosting-process memory — always show so absence is explicit.
    lines.push(detail_line(
        l.rss,
        debug
            .memory
            .process_rss_bytes
            .map_or_else(|| "N/A".to_string(), format_bytes),
        scheme,
    ));
    lines.push(detail_line(
        l.vm_size,
        debug
            .memory
            .process_vm_size_bytes
            .map_or_else(|| "N/A".to_string(), format_bytes),
        scheme,
    ));
    // Queue pressure — always show so absence of pressure is explicit.
    lines.push(detail_line(
        l.queue_depth,
        format!(
            "{} total, {} max",
            debug.actor_work_queue_depth_total, debug.actor_work_queue_depth_max
        ),
        scheme,
    ));
    // Retained queue-pressure evidence (PD-6, PD-7).
    if debug.actor_work_queue_depth_high_water_mark > 0 {
        lines.push(detail_line(
            l.peak_depth,
            debug.actor_work_queue_depth_high_water_mark.to_string(),
            scheme,
        ));
    }
    lines.push(detail_line(
        l.last_busy,
        match debug.last_nonzero_queue_depth_age_ms {
            None => "never".to_string(),
            Some(ms) if ms < 1000 => format!("{}ms ago", ms),
            Some(ms) => format!("{}s ago", ms / 1000),
        },
        scheme,
    ));
    lines.push(detail_line(
        l.data_as_of,
        format_system_time_relative(&payload.as_of),
        scheme,
    ));

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
            Span::raw(actor.to_string()),
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
const TOP_N: usize = 10;
const MIN_FLIGHT_RECORDER_HEIGHT: u16 = 5;
const RED_STALL_THRESHOLD: usize = 100; // placeholder; tune later.

#[allow(clippy::too_many_arguments)]
fn render_actor_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    payload: &NodePayload,
    actor_status: &str,
    actor_type: &str,
    instance_id: &str,
    queue_depth: u64,
    messages_processed: u64,
    created_at: &Option<SystemTime>,
    last_message_handler: Option<&str>,
    total_processing_time_us: u64,
    flight_recorder_json: Option<&str>,
    inbound_ordering: Option<&InboundOrdering>,
    execution: Option<&Execution>,
    failure_info: Option<&FailureInfo>,
    scheme: &ColorScheme,
    l: &Labels,
) {
    // info_height grew from 11/15 to 13/17 to accommodate two new
    // lines (Instance, Queue depth) in the info block.
    let info_height: u16 = if failure_info.is_some() { 17 } else { 13 };
    let ordering_height = compute_ordering_height(area.height, info_height, inbound_ordering);
    // Execution section is hidden (height 0) when the actor reports no
    // execution (None = unsupported) or nothing is in flight; its budget
    // comes after info + ordering, preserving the flight recorder's minimum.
    let execution_height =
        compute_execution_height(area.height, info_height, ordering_height, execution);
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(info_height),
            Constraint::Length(ordering_height),
            Constraint::Length(execution_height),
            Constraint::Min(MIN_FLIGHT_RECORDER_HEIGHT),
        ])
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

    let created_str = created_at
        .as_ref()
        .map(format_system_time_iso)
        .unwrap_or_else(|| "-".to_string());

    let mut lines = vec![
        Line::from(vec![
            Span::styled(l.status, scheme.detail_label),
            Span::styled(actor_status, status_style),
        ]),
        detail_line(l.instance_id_label, instance_id, scheme),
        detail_line(l.queue_depth, queue_depth.to_string(), scheme),
        detail_line(
            l.data_as_of,
            format_system_time_relative(&payload.as_of),
            scheme,
        ),
        detail_line(l.actor_type, actor_type, scheme),
        detail_line(l.messages, messages_processed.to_string(), scheme),
        detail_line(
            l.processing_time,
            humantime::format_duration(std::time::Duration::from_micros(total_processing_time_us))
                .to_string(),
            scheme,
        ),
        detail_line(l.created, &created_str, scheme),
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
            None => fi.root_cause_actor.to_string(),
        };
        lines.push(Line::from(vec![
            Span::styled(l.root_cause, scheme.detail_label),
            Span::raw(root_cause_display),
        ]));
        lines.push(detail_line(
            l.failed_at,
            format_system_time_iso(&fi.occurred_at),
            scheme,
        ));
        lines.push(Line::from(vec![
            Span::styled(l.propagated, scheme.detail_label),
            Span::raw(if fi.is_propagated { l.yes } else { l.no }),
        ]));
    }

    let info = Paragraph::new(lines)
        .block(info_block)
        .wrap(Wrap { trim: false });
    frame.render_widget(info, chunks[0]);

    // Inbound ordering
    render_inbound_ordering(frame, chunks[1], inbound_ordering, scheme, l);

    // Execution (hidden via a zero-height chunk when None / active_count 0).
    render_execution(frame, chunks[2], execution, scheme, l);

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
    frame.render_widget(recorder, chunks[3]);
}

/// Compute the desired height for the inbound-ordering section, capped
/// against the available area so the flight recorder's minimum height
/// (`MIN_FLIGHT_RECORDER_HEIGHT`) is preserved even when the desired
/// height would otherwise overflow.
fn compute_ordering_height(
    area_height: u16,
    info_height: u16,
    inbound_ordering: Option<&InboundOrdering>,
) -> u16 {
    let budget = area_height
        .saturating_sub(info_height)
        .saturating_sub(MIN_FLIGHT_RECORDER_HEIGHT);
    let want: u16 = match inbound_ordering {
        None => 3,
        Some(io) if !io.enabled => 3,
        Some(io) => {
            let stalled = io.sessions.iter().filter(|s| s.buffered_count > 0).count();
            if stalled == 0 {
                3 // border(2) + rollup line 1 only
            } else {
                let rows = stalled.min(TOP_N) as u16;
                let more_row = if stalled > TOP_N { 1 } else { 0 };
                // border(2) + rollup(2) + spacer(1) + header(1) + rows + footer(3)
                2 + 2 + 1 + 1 + rows + more_row + 3
            }
        }
    };
    want.min(budget)
}

/// Compute the desired height for the Execution section.
///
/// Returns 0 when the actor reports no execution (`None` = unsupported) or
/// nothing is in flight (`active_count == 0`), hiding the section entirely.
/// Otherwise sizes to the rendered line count, capped against the remaining
/// budget so the flight recorder keeps its minimum height. Must match the
/// line count produced by `build_execution_lines`.
fn compute_execution_height(
    area_height: u16,
    info_height: u16,
    ordering_height: u16,
    execution: Option<&Execution>,
) -> u16 {
    let Some(e) = execution else {
        return 0;
    };
    if e.active_count == 0 {
        return 0;
    }
    let budget = area_height
        .saturating_sub(info_height)
        .saturating_sub(ordering_height)
        .saturating_sub(MIN_FLIGHT_RECORDER_HEIGHT);
    // border(2) + count line(1) + oldest line(1 iff a row exists)
    // + per-name rows + truncation marker(1 iff truncated).
    let oldest_line = u16::from(!e.active_handlers.is_empty());
    let rows = e.active_handlers.len() as u16;
    let more_row = u16::from(e.truncated);
    let want = 2 + 1 + oldest_line + rows + more_row;
    want.min(budget)
}

/// Render the Execution section: the second plane to lifecycle status,
/// answering "what is this actor handling right now?".
///
/// Drawn only when the actor reports execution (`Some`) with
/// `active_count > 0` (the caller gives this a zero-height chunk
/// otherwise). The top-level count comes straight from `active_count` --
/// never summed from the (possibly truncated) per-name rows (EX-3).
fn render_execution(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    execution: Option<&Execution>,
    scheme: &ColorScheme,
    l: &Labels,
) {
    let Some(e) = execution else {
        return;
    };
    if e.active_count == 0 {
        return;
    }
    let block = Block::default()
        .title(l.pane_execution)
        .borders(Borders::ALL)
        .border_style(scheme.border);
    let p = Paragraph::new(build_execution_lines(e, scheme, l))
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(p, area);
}

fn build_execution_lines<'a>(
    e: &'a Execution,
    scheme: &ColorScheme,
    l: &'a Labels,
) -> Vec<Line<'a>> {
    let mut lines: Vec<Line> = Vec::new();

    // Top-level count: read directly from active_count (EX-3).
    lines.push(detail_line(
        l.execution_active_handlers,
        e.active_count.to_string(),
        scheme,
    ));

    // Oldest live invocation: the first row (aggregated by name and sorted
    // oldest-first upstream, EX-4). Absent when per-handler detail was
    // momentarily unavailable on this poll (EX-2: complete == false leaves
    // active_handlers empty while the count above stays authoritative).
    if let Some(first) = e.active_handlers.first() {
        lines.push(detail_line(
            l.execution_oldest,
            format!(
                "{} ({})",
                first.name,
                format_system_time_relative(&first.oldest_since)
            ),
            scheme,
        ));
    }

    // Per-name rows: "name ×active_count  age".
    for h in &e.active_handlers {
        lines.push(Line::from(Span::raw(format!(
            "{} \u{00d7}{}  {}",
            h.name,
            h.active_count,
            format_system_time_relative(&h.oldest_since),
        ))));
    }

    // Truncation marker (EX-4): some of the oldest names were dropped. We
    // don't carry a precise hidden-name count, so the marker is unqualified.
    if e.truncated {
        lines.push(Line::from(Span::styled(
            l.execution_more_names,
            scheme.detail_label,
        )));
    }

    lines
}

/// Column widths for the per-session table. Owner column truncates
/// long ActorAddrs (full address still observable via the API);
/// remaining columns sized for typical seq/count widths.
const OWNER_COL_WIDTH: usize = 30;
const NEED_SEQ_COL_WIDTH: usize = 8;

fn render_inbound_ordering(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    inbound_ordering: Option<&InboundOrdering>,
    scheme: &ColorScheme,
    l: &Labels,
) {
    // Red border when actionable stalls are present; otherwise
    // matches the rest of the chrome.
    let border_style = match inbound_ordering {
        Some(io) if io.enabled && io.returned_buffered_session_count > 0 => {
            scheme.detail_alert_border
        }
        _ => scheme.border,
    };
    let block = Block::default()
        .title(l.pane_inbound_ordering)
        .borders(Borders::ALL)
        .border_style(border_style);

    let lines: Vec<Line> = match inbound_ordering {
        None => vec![Line::from(Span::styled(
            l.ordering_not_available,
            scheme.detail_label,
        ))],
        Some(io) if !io.enabled => vec![Line::from(Span::styled(
            l.ordering_buffering_disabled,
            scheme.detail_label,
        ))],
        Some(io) => build_inbound_ordering_lines(io, scheme, l),
    };

    let p = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(p, area);
}

fn build_inbound_ordering_lines<'a>(
    io: &'a InboundOrdering,
    scheme: &ColorScheme,
    l: &'a Labels,
) -> Vec<Line<'a>> {
    let mut lines: Vec<Line> = Vec::new();

    // Filter to stalled-only, sort by severity then by oldest gap.
    let mut stalled: Vec<&hyperactor::ordering::OrderingSessionSnapshot> = io
        .sessions
        .iter()
        .filter(|s| s.buffered_count > 0)
        .collect();
    stalled.sort_by(|a, b| {
        b.buffered_count.cmp(&a.buffered_count).then_with(|| {
            match (a.oldest_buffered_seq, b.oldest_buffered_seq) {
                (Some(ax), Some(bx)) => ax.cmp(&bx),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        })
    });

    // Rollup line 1: "enabled · 2 of 3 sessions stalled (partial: N skipped)"
    let partial_suffix = if !io.snapshot_complete {
        format!(
            " {}",
            l.ordering_sessions_partial
                .replace("{n}", &io.skipped_session_count.to_string())
        )
    } else {
        String::new()
    };
    lines.push(Line::from(vec![
        Span::styled(l.ordering_buffering_enabled, scheme.detail_label),
        Span::raw(format!(
            " · {} {} {} {} {}{}",
            io.returned_buffered_session_count,
            l.ordering_sessions_known,
            io.known_session_count,
            l.ordering_sessions_label,
            l.ordering_sessions_stalled,
            partial_suffix,
        )),
    ]));

    // Rollup line 2 only when there's actually something buffered;
    // for clean snapshots line 1 already conveys "0 of N stalled".
    if io.returned_buffered_message_count > 0 {
        lines.push(Line::from(Span::raw(format!(
            "{} {}, {} {}{}",
            io.returned_buffered_message_count,
            l.ordering_buffered_label,
            l.ordering_max_in_worst,
            io.returned_max_buffered_count,
            l.ordering_returned_total,
        ))));
    }

    if stalled.is_empty() {
        return lines;
    }

    // Spacer + column header.
    lines.push(Line::default());
    lines.push(Line::from(Span::styled(
        format!(
            "{:<owner$} {:>seq$}  {}",
            l.ordering_col_owner,
            l.ordering_col_missing_seq,
            l.ordering_col_buffered,
            owner = OWNER_COL_WIDTH,
            seq = NEED_SEQ_COL_WIDTH,
        ),
        scheme.detail_label,
    )));

    // Per-session rows. Color is themed: stalled rows use
    // `scheme.detail_stalled`; severe stalls (above threshold) escalate
    // to `scheme.detail_stalled_severe` (adds bold).
    let visible = stalled.iter().take(TOP_N);
    for s in visible {
        let owner_full = match &s.sender {
            Some(addr) => trim_owner_for_display(&addr.to_string()),
            None => l
                .ordering_sender_fallback
                .replace("{id}", &short_session_id(&s.session_id.to_string())),
        };
        let owner = truncate_cell(&owner_full, OWNER_COL_WIDTH);
        let buffered_cell = match (s.oldest_buffered_seq, s.newest_buffered_seq) {
            (Some(oldest), Some(newest)) => {
                format!("{} ({}-{})", s.buffered_count, oldest, newest)
            }
            _ => s.buffered_count.to_string(),
        };
        let row_style = if s.buffered_count > RED_STALL_THRESHOLD {
            scheme.detail_stalled_severe
        } else {
            scheme.detail_stalled
        };
        let row_text = format!(
            "{:<owner$} {:>seq$}  {}",
            owner,
            s.expected_next_seq,
            buffered_cell,
            owner = OWNER_COL_WIDTH,
            seq = NEED_SEQ_COL_WIDTH,
        );
        lines.push(Line::from(Span::styled(row_text, row_style)));
    }
    if stalled.len() > TOP_N {
        let more = stalled.len() - TOP_N;
        lines.push(Line::from(Span::styled(
            l.ordering_more_row.replace("{n}", &more.to_string()),
            scheme.detail_label,
        )));
    }

    // Footer (only with stalled sessions visible — this branch).
    lines.push(Line::default());
    lines.push(Line::from(Span::styled(
        l.ordering_footer,
        scheme.detail_label,
    )));

    lines
}

/// Strip `<base58>` instance suffixes and the `@location` tail from an
/// `ActorAddr` string, leaving the human-meaningful
/// `{actor_label}.{proc_label}` form. Full address is still observable
/// via the API; this is the at-a-glance form for the TUI.
fn trim_owner_for_display(addr: &str) -> String {
    let pre_at = addr.split('@').next().unwrap_or(addr);
    let mut out = String::with_capacity(pre_at.len());
    let mut depth: i32 = 0;
    for c in pre_at.chars() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out
}

/// Truncate `s` to `width` chars; if shorter, returns owned copy.
/// Truncation marker is `…` to make the cut visible to operators.
fn truncate_cell(s: &str, width: usize) -> String {
    if s.chars().count() <= width {
        return s.to_string();
    }
    let kept: String = s.chars().take(width.saturating_sub(1)).collect();
    format!("{kept}…")
}

fn short_session_id(s: &str) -> String {
    s.chars().take(8).collect()
}

/// Build an `Overlay` from diagnostics state.
///
/// Called by `ActiveJob::build_overlay` for the `Diagnostics` variant.
/// Takes individual fields rather than `&App` so that the borrow
/// checker allows `rebuild_overlay` to hold `&self.active_job` (shared)
/// while writing to `self.overlay`.
pub(crate) fn build_diag_overlay(
    results: &[DiagResult],
    running: bool,
    completed_at: Option<&str>,
    theme: &Theme,
) -> crate::overlay::Overlay {
    let scheme = &theme.scheme;
    let labels = &theme.labels;
    let sep = labels.separator;

    // Title line.
    let title = if running {
        Line::from(vec![
            Span::styled(
                labels.pane_diagnostics,
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(format!("{}{}", sep, labels.diag_running), scheme.info),
        ])
    } else if let Some(t) = completed_at {
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
            Span::styled(t.to_string(), scheme.stat_timing),
            Span::styled(
                format!("{}{}", sep, labels.diag_static_snapshot),
                scheme.detail_stopped,
            ),
        ])
    } else {
        Line::from(Span::raw(labels.pane_diagnostics))
    };

    // Pinned status line.
    let status_line = if running {
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

    crate::overlay::Overlay {
        title,
        status_line: Some(status_line),
        lines,
        loading: running,
        scroll: std::cell::Cell::new(0),
        max_scroll: std::cell::Cell::new(u16::MAX),
    }
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
            DiagNodeRole::CastActor => labels.diag_note_cast_actor,
            DiagNodeRole::ProcAgent => labels.diag_note_proc_agent,
            DiagNodeRole::UserProc => labels.diag_note_user_proc,
            DiagNodeRole::UserActor => labels.diag_note_user_actor,
        };
        spans.push(Span::styled(format!("  — {}", note), scheme.stat_url));
    }
    Line::from(spans)
}

/// Render a generic scrollable overlay in the detail pane area.
///
/// Supports an optional pinned status line (used by diagnostics)
/// above the scrollable content.
fn render_overlay(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    overlay: &crate::overlay::Overlay,
    scheme: &ColorScheme,
) {
    let block = Block::default()
        .title(overlay.title.clone())
        .borders(Borders::ALL)
        .border_style(scheme.border);
    let inner = block.inner(area);
    frame.render_widget(block, area);

    // If there's a pinned status line, split inner into status + scrollable.
    let content_area = if let Some(status) = &overlay.status_line {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(inner);
        frame.render_widget(Paragraph::new(status.clone()), chunks[0]);
        chunks[1]
    } else {
        inner
    };

    // Show "fetching…" only when loading with no lines AND no status line.
    // When a status_line is present it already communicates the loading state
    // (e.g. "Running… • fetching stack trace"), so the placeholder is redundant.
    let lines: Vec<Line<'static>> =
        if overlay.loading && overlay.lines.is_empty() && overlay.status_line.is_none() {
            vec![Line::from("fetching…")]
        } else {
            overlay.lines.clone()
        };

    let max_scroll = (lines.len() as u16).saturating_sub(content_area.height);
    overlay.max_scroll.set(max_scroll);
    let scroll = overlay.scroll.get().min(max_scroll);
    overlay.scroll.set(scroll);

    let paragraph = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));
    frame.render_widget(paragraph, content_area);
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use hyperactor_mesh::introspect::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::theme::LangName;

    /// Size-aware variant: render at the requested terminal dimensions.
    /// Use a wide/tall size (e.g. 120x45) for tests that assert on
    /// long ActorAddr substrings or top-N tables; smaller sizes for
    /// degradation tests.
    fn render_detail_to_string_with_size(payload: &NodePayload, width: u16, height: u16) -> String {
        let theme = crate::Theme::new(crate::ThemeName::Nord, LangName::En);
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = frame.area();
                render_node_detail(frame, area, payload, &theme.scheme, &theme.labels);
            })
            .unwrap();
        let buf = terminal.backend().buffer().clone();
        let mut text = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                text.push(buf[(x, y)].symbol().chars().next().unwrap_or(' '));
            }
            text.push('\n');
        }
        text
    }

    /// Render a detail pane into a 60x30 test buffer and return the
    /// rendered text as a single string for assertion. Legacy size
    /// kept for backwards compatibility with existing host/proc/root
    /// tests; inbound-ordering tests use the size-aware variant.
    fn render_detail_to_string(payload: &NodePayload) -> String {
        render_detail_to_string_with_size(payload, 60, 30)
    }

    fn mock_host_ref() -> NodeRef {
        let proc_id = hyperactor_mesh::mesh_id::ResourceId::proc_addr_from_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            "world",
        );
        NodeRef::Host(proc_id.actor_addr("host_agent"))
    }

    fn mock_proc_ref() -> NodeRef {
        let proc_id = hyperactor_mesh::mesh_id::ResourceId::proc_addr_from_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            "worker",
        );
        NodeRef::Proc(proc_id)
    }

    // PD-*: host detail shows memory stats when present.
    #[test]
    fn render_host_detail_shows_memory_stats() {
        let payload = NodePayload {
            identity: mock_host_ref(),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8080".to_string(),
                num_procs: 3,
                system_children: vec![],
                memory: ProcessMemoryStats {
                    process_rss_bytes: Some(512 * 1024 * 1024), // 512 MiB
                    process_vm_size_bytes: Some(2 * 1024 * 1024 * 1024), // 2 GiB
                },
            },
            children: vec![],
            parent: Some(NodeRef::Root),
            as_of: SystemTime::now(),
        };
        let text = render_detail_to_string(&payload);
        assert!(text.contains("512.0 MiB"), "expected RSS in output: {text}");
        assert!(
            text.contains("2.0 GiB"),
            "expected VM Size in output: {text}"
        );
    }

    // PD-*: proc detail shows debug stats with non-default values.
    #[test]
    fn render_proc_detail_shows_debug_stats() {
        let payload = NodePayload {
            identity: mock_proc_ref(),
            properties: NodeProperties::Proc {
                proc_name: "worker".to_string(),
                num_actors: 10,
                system_children: vec![],
                stopped_children: vec![],
                stopped_retention_cap: 0,
                is_poisoned: false,
                failed_actor_count: 0,
                debug: ProcDebugStats {
                    memory: ProcessMemoryStats {
                        process_rss_bytes: Some(256 * 1024 * 1024),
                        process_vm_size_bytes: Some(1024 * 1024 * 1024),
                    },
                    actor_work_queue_depth_total: 42,
                    actor_work_queue_depth_max: 7,
                    actor_work_queue_depth_high_water_mark: 100,
                    last_nonzero_queue_depth_age_ms: Some(3000),
                },
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        };
        let text = render_detail_to_string(&payload);
        assert!(text.contains("256.0 MiB"), "expected RSS: {text}");
        assert!(text.contains("1.0 GiB"), "expected VM Size: {text}");
        assert!(text.contains("42 total"), "expected queue total: {text}");
        assert!(text.contains("7 max"), "expected queue max: {text}");
    }

    // PD-2: unavailable memory shows "N/A", not fake zeros.
    #[test]
    fn render_host_detail_shows_na_for_unavailable_memory() {
        let payload = NodePayload {
            identity: mock_host_ref(),
            properties: NodeProperties::Host {
                addr: "10.0.0.1:8080".to_string(),
                num_procs: 1,
                system_children: vec![],
                memory: ProcessMemoryStats {
                    process_rss_bytes: None,
                    process_vm_size_bytes: None,
                },
            },
            children: vec![],
            parent: Some(NodeRef::Root),
            as_of: SystemTime::now(),
        };
        let text = render_detail_to_string(&payload);
        assert!(
            text.contains("N/A"),
            "expected N/A for unavailable memory: {text}"
        );
    }

    // ---- inbound-ordering render tests (IO-1 / IO-2 / IO-6 / IO-7) ----

    fn mock_actor_addr(name: &str, proc_name: &str) -> hyperactor::ActorAddr {
        let proc_id = hyperactor_mesh::mesh_id::ResourceId::proc_addr_from_name(
            "unix:@test"
                .parse::<hyperactor::channel::ChannelAddr>()
                .unwrap(),
            proc_name,
        );
        proc_id.actor_addr(name)
    }

    fn mock_actor_ref(name: &str) -> NodeRef {
        NodeRef::Actor(mock_actor_addr(name, "world"))
    }

    fn actor_payload(inbound_ordering: Option<Box<InboundOrdering>>) -> NodePayload {
        NodePayload {
            identity: mock_actor_ref("stalled_receiver"),
            properties: NodeProperties::Actor {
                actor_status: "idle".to_string(),
                actor_type: "monarch::test::TestActor".to_string(),
                instance_id: "019e5661-7d33-7380-9afe-699ffc567531".to_string(),
                messages_processed: 1,
                created_at: None,
                last_message_handler: None,
                total_processing_time_us: 0,
                queue_depth: 8,
                flight_recorder: None,
                is_system: false,
                inbound_ordering,
                failure_info: None,
                execution: None,
            },
            children: vec![],
            parent: None,
            as_of: SystemTime::now(),
        }
    }

    fn actor_payload_with_execution(execution: Execution) -> NodePayload {
        let mut payload = actor_payload(None);
        if let NodeProperties::Actor { execution: e, .. } = &mut payload.properties {
            *e = Some(Box::new(execution));
        }
        payload
    }

    // EX-1: a supported-but-idle actor (Some, active_count 0) hides the
    // Execution section -- no border title, no count line.
    #[test]
    fn render_actor_detail_execution_hidden_when_idle() {
        let idle = Execution {
            active_count: 0,
            active_handlers: vec![],
            complete: true,
            truncated: false,
        };
        let text = render_detail_to_string_with_size(&actor_payload_with_execution(idle), 120, 45);
        assert!(
            !text.contains("Active handlers"),
            "Execution section must be hidden at active_count 0: {text}"
        );
    }

    // Populated: the count line reads active_count directly (2), the oldest
    // line names the oldest endpoint, and the aggregated per-name row shows
    // "hold ×2".
    #[test]
    fn render_actor_detail_execution_populated() {
        let execution = Execution {
            active_count: 2,
            active_handlers: vec![ActiveHandler {
                name: "hold".to_string(),
                active_count: 2,
                oldest_since: SystemTime::now(),
            }],
            complete: true,
            truncated: false,
        };
        let text =
            render_detail_to_string_with_size(&actor_payload_with_execution(execution), 120, 45);
        assert!(
            text.contains("Active handlers: 2"),
            "expected count line from active_count: {text}"
        );
        assert!(
            text.contains("Oldest: hold"),
            "expected oldest endpoint name: {text}"
        );
        assert!(
            text.contains("hold \u{00d7}2"),
            "expected aggregated row 'hold ×2': {text}"
        );
        assert!(
            !text.contains("more endpoint names"),
            "no truncation marker when not truncated: {text}"
        );
    }

    // Truncation: the count line still reads the FULL total (200); the marker
    // appears (we carry no precise hidden-name count, so it is unqualified).
    #[test]
    fn render_actor_detail_execution_truncated_marker() {
        let mut active_handlers = Vec::new();
        for i in 0..16u64 {
            active_handlers.push(ActiveHandler {
                name: format!("ep_{i:02}"),
                active_count: 1,
                oldest_since: SystemTime::now(),
            });
        }
        let execution = Execution {
            active_count: 200,
            active_handlers,
            complete: true,
            truncated: true,
        };
        let text =
            render_detail_to_string_with_size(&actor_payload_with_execution(execution), 120, 60);
        assert!(
            text.contains("Active handlers: 200"),
            "count line is the full total, not the row count: {text}"
        );
        assert!(
            text.contains("more endpoint names"),
            "truncation marker present when truncated: {text}"
        );
    }

    fn session(
        session_id: uuid::Uuid,
        sender: Option<hyperactor::ActorAddr>,
        buffered_count: usize,
        last_released: u64,
        oldest: Option<u64>,
        newest: Option<u64>,
    ) -> hyperactor::ordering::OrderingSessionSnapshot {
        hyperactor::ordering::OrderingSessionSnapshot {
            session_id,
            sender,
            last_released_seq: last_released,
            expected_next_seq: last_released + 1,
            buffered_count,
            oldest_buffered_seq: oldest,
            newest_buffered_seq: newest,
        }
    }

    // IO-1: structural absence renders "not available".
    #[test]
    fn render_actor_detail_inbound_ordering_none() {
        let text = render_detail_to_string_with_size(&actor_payload(None), 120, 45);
        assert!(
            text.contains("not available"),
            "expected 'not available' in output: {text}"
        );
        assert!(
            !text.contains("Owner"),
            "no per-session header in None state: {text}"
        );
    }

    // IO-1 enabled=false: shows compact "disabled (direct_send)" line.
    #[test]
    fn render_actor_detail_inbound_ordering_disabled() {
        let io = InboundOrdering {
            enabled: false,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 0,
            returned_buffered_session_count: 0,
            returned_buffered_message_count: 0,
            returned_max_buffered_count: 0,
            sessions: vec![],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        assert!(
            text.contains("disabled (direct_send)"),
            "expected compact disabled message: {text}"
        );
        assert!(
            !text.contains("Need seq"),
            "no per-session header in disabled state: {text}"
        );
    }

    // Enabled but no stalled sessions: line 1 only; no buffered totals line; no table.
    #[test]
    fn render_actor_detail_inbound_ordering_enabled_no_stalled() {
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 2,
            returned_buffered_session_count: 0,
            returned_buffered_message_count: 0,
            returned_max_buffered_count: 0,
            sessions: vec![
                session(uuid::Uuid::nil(), None, 0, 5, None, None),
                session(uuid::Uuid::from_u128(1), None, 0, 3, None, None),
            ],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        assert!(text.contains("enabled"), "expected enabled label: {text}");
        assert!(
            text.contains("0 of 2 sessions stalled"),
            "expected rollup '0 of 2 sessions stalled': {text}"
        );
        assert!(
            !text.contains("Need seq"),
            "no per-session table header in clean state: {text}"
        );
    }

    // One stalled session: rollup, table row, footer all present.
    #[test]
    fn render_actor_detail_inbound_ordering_one_stalled() {
        let sender = mock_actor_addr("sender_a", "sender_a_proc");
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 1,
            returned_buffered_session_count: 1,
            returned_buffered_message_count: 5,
            returned_max_buffered_count: 5,
            sessions: vec![session(
                uuid::Uuid::nil(),
                Some(sender),
                5,
                0,
                Some(2),
                Some(6),
            )],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        assert!(
            text.contains("1 of 1 sessions stalled"),
            "expected '1 of 1 sessions stalled': {text}"
        );
        assert!(text.contains("Need seq"), "expected column header: {text}");
        assert!(text.contains("sender_a"), "expected sender label: {text}");
        assert!(
            text.contains("5 (2-6)"),
            "expected buffered range '5 (2-6)': {text}"
        );
        assert!(
            text.contains("Owner = SEQ_INFO session owner"),
            "expected footer: {text}"
        );
    }

    // IO-2: partial snapshot suffix appears.
    #[test]
    fn render_actor_detail_inbound_ordering_partial_snapshot() {
        let sender = mock_actor_addr("sender_a", "sender_a_proc");
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: false,
            skipped_session_count: 2,
            known_session_count: 3,
            returned_buffered_session_count: 1,
            returned_buffered_message_count: 3,
            returned_max_buffered_count: 3,
            sessions: vec![session(
                uuid::Uuid::nil(),
                Some(sender),
                3,
                0,
                Some(2),
                Some(4),
            )],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        assert!(
            text.contains("(partial: 2 skipped)"),
            "expected partial-snapshot suffix: {text}"
        );
    }

    // Top-N truncation: 12 stalled sessions render top 10 + "... and 2 more".
    #[test]
    fn render_actor_detail_inbound_ordering_top_n_truncation() {
        let mut sessions = Vec::new();
        for i in 0..12u64 {
            let sender = mock_actor_addr(&format!("s{i}"), "p");
            // Larger i → larger buffered_count, so sort surfaces s11 first.
            let buffered = (i as usize) + 1;
            sessions.push(session(
                uuid::Uuid::from_u128(i as u128),
                Some(sender),
                buffered,
                0,
                Some(2),
                Some(1 + buffered as u64),
            ));
        }
        let total: usize = sessions.iter().map(|s| s.buffered_count).sum();
        let max = sessions.iter().map(|s| s.buffered_count).max().unwrap_or(0);
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 12,
            returned_buffered_session_count: 12,
            returned_buffered_message_count: total,
            returned_max_buffered_count: max,
            sessions,
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        // s11 (buffered=12) is the largest; s2 (buffered=3) is the 10th largest.
        assert!(text.contains("s11"), "expected biggest session row: {text}");
        assert!(
            text.contains("s2"),
            "expected 10th-largest session row: {text}"
        );
        assert!(
            !text.contains("s1 ") && !text.contains("s0 "),
            "smallest sessions should not appear as rows: {text}"
        );
        assert!(
            text.contains("and 2 more"),
            "expected '… and 2 more' footer: {text}"
        );
    }

    // sender == None: row uses fallback label derived from session_id.
    #[test]
    fn render_actor_detail_inbound_ordering_sender_none_fallback() {
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 1,
            returned_buffered_session_count: 1,
            returned_buffered_message_count: 2,
            returned_max_buffered_count: 2,
            sessions: vec![session(uuid::Uuid::nil(), None, 2, 0, Some(2), Some(3))],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        assert!(
            text.contains("no owner") && text.contains("00000000"),
            "expected fallback owner label with short session id: {text}"
        );
    }

    // Sort: larger buffered_count first; tiebreak by oldest_buffered_seq asc.
    #[test]
    fn render_actor_detail_inbound_ordering_sort_order() {
        let s_large = session(
            uuid::Uuid::from_u128(1),
            Some(mock_actor_addr("big", "p")),
            10,
            0,
            Some(2),
            Some(11),
        );
        // Actor labels are lowercased by `Label::strip`, so use
        // lowercase-stable names here to keep assertions reliable.
        let s_small_oldest_low = session(
            uuid::Uuid::from_u128(2),
            Some(mock_actor_addr("small_a", "p")),
            5,
            0,
            Some(2),
            Some(6),
        );
        let s_small_oldest_high = session(
            uuid::Uuid::from_u128(3),
            Some(mock_actor_addr("small_b", "p")),
            5,
            0,
            Some(50),
            Some(54),
        );
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 3,
            returned_buffered_session_count: 3,
            returned_buffered_message_count: 20,
            returned_max_buffered_count: 10,
            sessions: vec![s_small_oldest_high, s_large, s_small_oldest_low],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);
        let big_pos = text.find("big").expect("expected 'big' in output");
        let small_a_pos = text.find("small_a").expect("expected 'small_a' in output");
        let small_b_pos = text.find("small_b").expect("expected 'small_b' in output");
        assert!(
            big_pos < small_a_pos && big_pos < small_b_pos,
            "expected 'big' (buffered=10) first; got big@{big_pos}, small_a@{small_a_pos}, small_b@{small_b_pos}: {text}"
        );
        assert!(
            small_a_pos < small_b_pos,
            "expected small_a (oldest=2) before small_b (oldest=50): {text}"
        );
    }

    // Info block exposes Instance: and Queue depth: scalars.
    #[test]
    fn render_actor_detail_info_block_shows_instance_and_queue_depth() {
        let text = render_detail_to_string_with_size(&actor_payload(None), 120, 45);
        assert!(
            text.contains("Instance:") && text.contains("019e5661"),
            "expected Instance: line with id: {text}"
        );
        assert!(
            text.contains("Queue depth:") && text.contains(" 8"),
            "expected Queue depth: 8 line: {text}"
        );
    }

    // Workload-shape live-acceptance fixture: mirrors MIT-78 deterministic
    // shape (known=3, two stalled sender sessions, one idle client session).
    #[test]
    fn render_actor_detail_inbound_ordering_workload_fixture() {
        let client = session(uuid::Uuid::from_u128(1), None, 0, 1, None, None);
        let sender_a = session(
            uuid::Uuid::from_u128(2),
            Some(mock_actor_addr("sender_a", "sender_a_proc")),
            5,
            0,
            Some(2),
            Some(6),
        );
        let sender_b = session(
            uuid::Uuid::from_u128(3),
            Some(mock_actor_addr("sender_b", "sender_b_proc")),
            3,
            0,
            Some(2),
            Some(4),
        );
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 3,
            returned_buffered_session_count: 2,
            returned_buffered_message_count: 8,
            returned_max_buffered_count: 5,
            sessions: vec![client, sender_a, sender_b],
        };
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 45);

        // Rollup line 1: stalled count of total.
        assert!(
            text.contains("2 of 3 sessions stalled"),
            "expected rollup '2 of 3 sessions stalled': {text}"
        );
        // Rollup line 2: buffered totals.
        assert!(
            text.contains("8 buffered") && text.contains("max 5/session"),
            "expected '8 buffered, max 5/session': {text}"
        );

        // Both sender rows present.
        let pos_a = text.find("sender_a").expect("expected sender_a row");
        let pos_b = text.find("sender_b").expect("expected sender_b row");
        assert!(
            pos_a < pos_b,
            "sender_a (buffered=5) should appear before sender_b (buffered=3): {text}"
        );

        // Idle client session NOT shown as a row.
        assert!(
            !text.contains("(no owner"),
            "idle client.local session must not be rendered as a row: {text}"
        );

        // Buffered cells.
        assert!(
            text.contains("5 (2-6)"),
            "expected sender_a buffered '5 (2-6)': {text}"
        );
        assert!(
            text.contains("3 (2-4)"),
            "expected sender_b buffered '3 (2-4)': {text}"
        );

        // Footer present.
        assert!(
            text.contains("Owner = SEQ_INFO session owner"),
            "expected footer in active-stalled state: {text}"
        );
    }

    // Cramped height: flight recorder section preserved even when the
    // ordering section's want exceeds the budget (closes C9 cap).
    #[test]
    fn render_actor_detail_inbound_ordering_cramped_height() {
        let mut sessions = Vec::new();
        for i in 0..10u64 {
            sessions.push(session(
                uuid::Uuid::from_u128(i as u128),
                Some(mock_actor_addr(&format!("s{i}"), "p")),
                10,
                0,
                Some(2),
                Some(11),
            ));
        }
        let io = InboundOrdering {
            enabled: true,
            snapshot_complete: true,
            skipped_session_count: 0,
            known_session_count: 10,
            returned_buffered_session_count: 10,
            returned_buffered_message_count: 100,
            returned_max_buffered_count: 10,
            sessions,
        };
        // Budget: 28 - info(13) - min_flight(5) = 10 rows for ordering,
        // far less than the want of 2+2+1+1+10+3 = 19. Cap kicks in.
        let text = render_detail_to_string_with_size(&actor_payload(Some(Box::new(io))), 120, 28);
        assert!(
            text.contains("Flight Recorder"),
            "expected flight recorder pane still visible under cramped height: {text}"
        );
    }
}
