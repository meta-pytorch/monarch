/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::cell::Cell;

use chrono::Local;
use ratatui::style::Modifier;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::diagnostics::DiagResult;
use crate::overlay::Overlay;
use crate::render::detail_pane::build_diag_overlay;
use crate::theme::Labels;
use crate::theme::Theme;

/// An in-progress overlay-producing async job.
///
/// Exactly one variant is live at a time. `App` holds
/// `active_job: Option<ActiveJob>` which is always `Some` iff
/// `overlay` is `Some` (TUI-21). Enforced by `set_job`/`dismiss_job`.
pub(crate) enum ActiveJob {
    /// A streaming diagnostic run. `running` flips to `false` and
    /// `rx` to `None` when the mpsc sender closes.
    Diagnostics {
        results: Vec<DiagResult>,
        running: bool,
        rx: Option<mpsc::Receiver<DiagResult>>,
        completed_at: Option<String>,
    },
    /// A single py-spy HTTP fetch.
    ///
    /// Use `rx.is_some()` — not `lines.is_empty()` — to distinguish
    /// "loading" from "completed with no output". An empty `lines`
    /// with `rx == None` means the fetch completed but returned nothing.
    PySpy {
        /// `Some` while the HTTP fetch is in flight; `None` after the
        /// oneshot fires.
        rx: Option<oneshot::Receiver<PySpyJobResult>>,
        short: String,
        /// Populated when the oneshot result arrives.
        lines: Vec<Line<'static>>,
        /// Set to a formatted timestamp when the result arrives.
        completed_at: Option<String>,
        /// Service proc reference used for managed py-spy
        /// provisioning. Set only when the target itself is the
        /// service proc.
        service_proc_ref: Option<String>,
        /// Operator-facing provisioning state for managed py-spy.
        provision_state: ProvisionState,
    },
    /// A single config dump HTTP fetch.
    ///
    /// Same rx/lines semantics as PySpy: `rx.is_some()` → loading.
    Config {
        rx: Option<oneshot::Receiver<Vec<Line<'static>>>>,
        short: String,
        lines: Vec<Line<'static>>,
        completed_at: Option<String>,
    },
}

impl ActiveJob {
    /// Build the display overlay for this job's current state.
    ///
    /// Takes `(&self, &Theme)` not `(&self, &App)` because
    /// `rebuild_overlay` borrows `self.active_job` (shared) while
    /// holding `&mut self` for the overlay assignment. The borrow
    /// checker rejects `&App` here.
    pub(crate) fn build_overlay(&self, theme: &Theme) -> Overlay {
        match self {
            ActiveJob::Diagnostics {
                results,
                running,
                completed_at,
                ..
            } => build_diag_overlay(results, *running, completed_at.as_deref(), theme),
            ActiveJob::PySpy {
                rx,
                short,
                lines,
                completed_at,
                provision_state,
                ..
            } => {
                let scheme = &theme.scheme;
                let labels = &theme.labels;
                let sep = labels.separator;
                // Use rx.is_some() — not lines.is_empty() — to detect
                // loading state. An empty lines with rx == None means
                // "completed with no output".
                let loading = rx.is_some();

                let title = if loading {
                    Line::from(vec![
                        Span::styled(
                            format!("py-spy: {short}"),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(format!("{sep}{}", labels.diag_running), scheme.info),
                    ])
                } else if let Some(ts) = completed_at {
                    Line::from(vec![
                        Span::styled(
                            format!("py-spy: {short}"),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format!("{sep}{}", labels.diag_completed_at),
                            scheme.detail_label,
                        ),
                        Span::raw(" "),
                        Span::styled(ts.clone(), scheme.stat_timing),
                    ])
                } else {
                    Line::from(Span::styled(
                        format!("py-spy: {short}"),
                        Style::default().add_modifier(Modifier::BOLD),
                    ))
                };

                let status_line = if loading {
                    Some(Line::from(vec![
                        Span::styled(labels.diag_running, scheme.info),
                        Span::styled(format!("{sep}fetching stack trace"), scheme.detail_label),
                    ]))
                } else {
                    None
                };

                let mut overlay_lines = lines.clone();
                append_provision_lines(&mut overlay_lines, provision_state, scheme);

                Overlay {
                    title,
                    status_line,
                    lines: overlay_lines,
                    loading,
                    scroll: Cell::new(0),
                    max_scroll: Cell::new(u16::MAX),
                }
            }
            ActiveJob::Config {
                rx,
                short,
                lines,
                completed_at,
            } => {
                let scheme = &theme.scheme;
                let labels = &theme.labels;
                let sep = labels.separator;
                let loading = rx.is_some();

                let title = if loading {
                    Line::from(vec![
                        Span::styled(
                            format!("config: {short}"),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(format!("{sep}{}", labels.diag_running), scheme.info),
                    ])
                } else if let Some(ts) = completed_at {
                    Line::from(vec![
                        Span::styled(
                            format!("config: {short}"),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            format!("{sep}{}", labels.diag_completed_at),
                            scheme.detail_label,
                        ),
                        Span::raw(" "),
                        Span::styled(ts.clone(), scheme.stat_timing),
                    ])
                } else {
                    Line::from(Span::styled(
                        format!("config: {short}"),
                        Style::default().add_modifier(Modifier::BOLD),
                    ))
                };

                let status_line = if loading {
                    Some(Line::from(vec![
                        Span::styled(labels.diag_running, scheme.info),
                        Span::styled(
                            format!("{sep}fetching config snapshot"),
                            scheme.detail_label,
                        ),
                    ]))
                } else {
                    None
                };

                Overlay {
                    title,
                    status_line,
                    lines: lines.clone(),
                    loading,
                    scroll: Cell::new(0),
                    max_scroll: Cell::new(u16::MAX),
                }
            }
        }
    }

    /// Return the footer help text for the current overlay state.
    /// Falls back to the default topology help text when no job is active.
    pub(crate) fn footer_text<'a>(job: &Option<Self>, labels: &'a Labels) -> &'a str {
        match job {
            Some(ActiveJob::Diagnostics { running: true, .. }) => {
                labels.footer_diag_running_help_text
            }
            Some(ActiveJob::Diagnostics { .. }) => labels.footer_diag_completed_help_text,
            Some(ActiveJob::PySpy {
                provision_state, ..
            }) => match provision_state {
                ProvisionState::CanProvision => labels.footer_pyspy_can_provision_help_text,
                ProvisionState::Provisioning { .. } => labels.footer_pyspy_provisioning_help_text,
                ProvisionState::Provisioned { .. } => labels.footer_pyspy_provisioned_help_text,
                ProvisionState::Idle | ProvisionState::Failed { .. } => {
                    labels.footer_pyspy_help_text
                }
            },
            Some(ActiveJob::Config { .. }) => labels.footer_config_help_text,
            None => labels.footer_help_text,
        }
    }

    /// Process an event from the async receiver, updating job state.
    /// Caller always rebuilds the overlay afterward.
    pub(crate) fn on_event(&mut self, event: ActiveJobEvent) {
        match event {
            ActiveJobEvent::DiagResult(Some(r)) => {
                if let ActiveJob::Diagnostics { results, .. } = self {
                    results.push(r);
                } else {
                    debug_assert!(false, "DiagResult delivered to non-Diagnostics job");
                }
            }
            ActiveJobEvent::DiagResult(None) => {
                if let ActiveJob::Diagnostics {
                    running,
                    rx,
                    completed_at,
                    ..
                } = self
                {
                    *running = false;
                    *rx = None;
                    *completed_at = Some(Local::now().format("%H:%M:%S").to_string());
                } else {
                    debug_assert!(false, "DiagResult(None) delivered to non-Diagnostics job");
                }
            }
            ActiveJobEvent::PySpyResult(result) => {
                if let ActiveJob::PySpy {
                    rx,
                    lines,
                    completed_at,
                    service_proc_ref,
                    provision_state,
                    ..
                } = self
                {
                    *rx = None;
                    *lines = result.lines;
                    *completed_at = Some(Local::now().format("%H:%M:%S").to_string());
                    if result.binary_not_found && service_proc_ref.is_some() {
                        *provision_state = ProvisionState::CanProvision;
                    } else {
                        *provision_state = ProvisionState::Idle;
                    }
                } else {
                    debug_assert!(false, "PySpyResult delivered to non-PySpy job");
                }
            }
            ActiveJobEvent::ProvisionResult(outcome) => {
                if let ActiveJob::PySpy {
                    provision_state, ..
                } = self
                {
                    *provision_state = match outcome {
                        ProvisionOutcome::Ok { executable } => {
                            ProvisionState::Provisioned { executable }
                        }
                        ProvisionOutcome::Err(error) => ProvisionState::Failed { error },
                    };
                } else {
                    debug_assert!(false, "ProvisionResult delivered to non-PySpy job");
                }
            }
            ActiveJobEvent::ConfigResult(new_lines) => {
                if let ActiveJob::Config {
                    rx,
                    lines,
                    completed_at,
                    ..
                } = self
                {
                    *rx = None;
                    *lines = new_lines;
                    *completed_at = Some(Local::now().format("%H:%M:%S").to_string());
                } else {
                    debug_assert!(false, "ConfigResult delivered to non-Config job");
                }
            }
        }
    }
}

/// Result of the active overlay-producing job completing one event.
pub(crate) enum ActiveJobEvent {
    DiagResult(Option<DiagResult>),
    PySpyResult(PySpyJobResult),
    ConfigResult(Vec<Line<'static>>),
    ProvisionResult(ProvisionOutcome),
}

/// TUI-level py-spy fetch result.
///
/// This is a rendering envelope, not the backend `PySpyResult` wire
/// type. It carries both display lines and the single semantic bit the
/// TUI needs for PY-6/PY-7: whether the backend reported that py-spy
/// was missing.
pub(crate) struct PySpyJobResult {
    /// Styled lines to render in the py-spy overlay.
    pub(crate) lines: Vec<Line<'static>>,
    /// True only for backend `BinaryNotFound` responses.
    pub(crate) binary_not_found: bool,
}

/// Managed py-spy provisioning state attached to the py-spy overlay.
pub(crate) enum ProvisionState {
    /// No provisioning affordance is currently visible.
    Idle,
    /// The current service-proc py-spy result is eligible for
    /// explicit operator-triggered provisioning (PY-6/PY-7).
    CanProvision,
    /// A provisioning POST is in flight. The receiver lives in the
    /// active py-spy job so stale results are dropped if the overlay
    /// is replaced (PY-5/PY-8).
    Provisioning {
        /// Completion channel for the async provisioning request.
        rx: oneshot::Receiver<ProvisionOutcome>,
    },
    /// Provisioning succeeded and the resolved executable path is
    /// available for display before the operator retries py-spy.
    Provisioned {
        /// Host-local executable path returned by the tool
        /// provisioning actor.
        executable: String,
    },
    /// Provisioning failed with an operator-readable error.
    Failed {
        /// Error text rendered in the existing py-spy overlay.
        error: String,
    },
}

/// Result of the async provisioning request.
pub(crate) enum ProvisionOutcome {
    /// The tool is available on the host.
    Ok {
        /// Host-local executable path returned by the backend.
        executable: String,
    },
    /// Provisioning failed before an executable could be resolved.
    Err(String),
}

fn append_provision_lines(
    lines: &mut Vec<Line<'static>>,
    provision_state: &ProvisionState,
    scheme: &crate::theme::ColorScheme,
) {
    match provision_state {
        ProvisionState::Idle => {}
        ProvisionState::CanProvision => {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "press P to provision managed py-spy on this host",
                scheme.info,
            )));
        }
        ProvisionState::Provisioning { .. } => {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "provisioning py-spy 0.4.1...",
                scheme.info,
            )));
        }
        ProvisionState::Provisioned { executable } => {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "managed py-spy provisioned",
                scheme.detail_status_ok,
            )));
            lines.push(Line::from(vec![
                Span::styled("executable: ", scheme.detail_label),
                Span::raw(executable.clone()),
            ]));
            lines.push(Line::from(Span::styled(
                "press p to retry dump",
                scheme.info,
            )));
        }
        ProvisionState::Failed { error } => {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                format!("managed py-spy provision failed: {error}"),
                scheme.error,
            )));
        }
    }
}
